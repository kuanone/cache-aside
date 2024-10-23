package cache_aside

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/redis/rueidis"
	"golang.org/x/sync/singleflight"
)

type CacheAsideKey interface {
	ParseCacheKey(cacheKey string) error
	EncodeCacheKey() string
}

type MetaLoader[T CacheAsideKey] interface {
	CacheAsideLoad(context.Context, T) (T, error)
}

type CacheAside[T CacheAsideKey] struct {
	redisClient   rueidis.Client
	metaLoader    MetaLoader[T]
	expireTime    time.Duration
	sf            singleflight.Group
	localCache    *cache.Cache
	useLocalCache bool
}

type CacheAsideOption func(*CacheAsideOptions)

type CacheAsideOptions struct {
	UseLocalCache             bool
	LocalCacheExpiration      time.Duration
	LocalCacheCleanupInterval time.Duration
}

func WithLocalCache(use bool, expiration, cleanupInterval time.Duration) CacheAsideOption {
	return func(o *CacheAsideOptions) {
		o.UseLocalCache = use
		o.LocalCacheExpiration = expiration
		o.LocalCacheCleanupInterval = cleanupInterval
	}
}

func NewCacheAside[T CacheAsideKey](redisClient rueidis.Client, metaLoader MetaLoader[T], expireTime time.Duration, opts ...CacheAsideOption) *CacheAside[T] {
	if redisClient == nil {
		panic("redisClient is nil")
	}

	if metaLoader == nil {
		panic("metaLoader is nil")
	}

	options := &CacheAsideOptions{
		UseLocalCache:             false,
		LocalCacheExpiration:      5 * time.Minute,
		LocalCacheCleanupInterval: 10 * time.Minute,
	}

	for _, opt := range opts {
		opt(options)
	}

	ca := &CacheAside[T]{
		redisClient:   redisClient,
		metaLoader:    metaLoader,
		expireTime:    expireTime,
		sf:            singleflight.Group{},
		useLocalCache: options.UseLocalCache,
	}

	if options.UseLocalCache {
		ca.localCache = cache.New(options.LocalCacheExpiration, options.LocalCacheCleanupInterval)
	}

	return ca
}

func (c *CacheAside[T]) load(ctx context.Context, expireTime time.Duration, cacheKey string, queryFunc func(ctx context.Context, metaLoader MetaLoader[T], cacheKey string) (T, error), result T) (T, error) {
	if c.useLocalCache {
		if cachedVal, found := c.localCache.Get(cacheKey); found {
			if err := json.Unmarshal(cachedVal.([]byte), &result); err == nil {
				return result, nil
			}
		}
	}

	val, err := c.redisClient.Do(ctx, c.redisClient.B().Get().Key(cacheKey).Build()).ToString()
	if err == nil && val != "" {
		err = json.Unmarshal([]byte(val), &result)
		if err == nil {
			if c.useLocalCache {
				c.localCache.Set(cacheKey, []byte(val), cache.DefaultExpiration)
			}
			return result, nil
		}
	}

	result, err = queryFunc(ctx, c.metaLoader, cacheKey)
	if err != nil {
		return result, err
	}

	data, err := json.Marshal(result)
	if err != nil {
		return result, err
	}

	var et time.Duration
	if expireTime < 1*time.Second {
		et = c.expireTime
	} else {
		et = expireTime
	}

	err = c.redisClient.Do(ctx, c.redisClient.B().Set().Key(cacheKey).Value(string(data)).Ex(et).Build()).Error()
	if err != nil {
		return result, err
	}

	if c.useLocalCache {
		c.localCache.Set(cacheKey, data, cache.DefaultExpiration)
	}

	return result, nil
}

func (c *CacheAside[T]) Query(ctx context.Context, expireTime time.Duration, cacheKey string, v T) (T, error) {
	o, err, _ := c.sf.Do(cacheKey, func() (interface{}, error) {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Pointer || rv.IsNil() {
			return v, errors.New("v must be a pointer")
		}
		qFunc := func(ctx context.Context, loader MetaLoader[T], cacheKey string) (T, error) {
			err := v.ParseCacheKey(cacheKey)
			if err != nil {
				return v, err
			}

			if loader == nil {
				return v, errors.New("metaLoader is nil")
			}

			return loader.CacheAsideLoad(ctx, v)
		}

		return c.load(ctx, expireTime, cacheKey, qFunc, v)
	})
	if err != nil {
		return v, err
	}
	return o.(T), nil
}
