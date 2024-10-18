package cache_aside

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/rueidis"
	"reflect"
	"time"
)

type CacheAsideKey interface {
	ParseCacheKey(cacheKey string) error
	EncodeCacheKey() string
}

type MetaLoader[T CacheAsideKey] interface {
	CacheAsideLoad(context.Context, T) (T, error)
}

type CacheAside[T CacheAsideKey] struct {
	redisClient rueidis.Client
	metaLoader  MetaLoader[T]
	expireTime  time.Duration
}

func NewCacheAside[T CacheAsideKey](redisClient rueidis.Client, metaLoader MetaLoader[T], expireTime time.Duration) *CacheAside[T] {
	if redisClient == nil {
		panic("redisClient is nil")
	}
	if metaLoader == nil {
		panic("metaLoader is nil")
	}
	return &CacheAside[T]{
		redisClient: redisClient,
		metaLoader:  metaLoader,
		expireTime:  expireTime,
	}
}

func (c *CacheAside[T]) load(ctx context.Context, expireTime time.Duration, cacheKey string, queryFunc func(ctx context.Context, metaLoader MetaLoader[T], cacheKey string) (T, error), result T) (T, error) {
	val, err := c.redisClient.Do(ctx, c.redisClient.B().Get().Key(cacheKey).Build()).ToString()
	if err == nil && val != "" {
		err = json.Unmarshal([]byte(val), &result)
		if err == nil {
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

	return result, nil
}

func (c *CacheAside[T]) Query(ctx context.Context, expireTime time.Duration, cacheKey string, v T) (T, error) {
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
}
