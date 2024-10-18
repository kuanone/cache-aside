package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/kuanone/cache-aside"
	"github.com/redis/rueidis"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

type User struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (u *User) ParseCacheKey(cacheKey string) error {
	ks := strings.Split(cacheKey, ":")
	if len(ks) == 0 {
		return errors.New("invalid cacheKey:" + cacheKey)
	}

	if ks[0] != "user" {
		return errors.New("invalid cacheKey:" + cacheKey)
	}
	var err error
	u.ID, err = strconv.ParseInt(ks[1], 10, 64)

	if u.ID == 0 {
		return errors.New("invalid cacheKey:" + cacheKey)
	}

	return err
}

func (u *User) EncodeCacheKey() string {
	return fmt.Sprintf("user:%d", u.ID)
}

func (u *User) CacheAsideLoad(ctx context.Context, i *User) (*User, error) {
	slog.Info("user db load", slog.Any("id", i.ID), slog.Any("name", i.Name))
	i.Name = "test" + strconv.FormatInt(i.ID, 10)
	i.Age = 18
	return i, nil
}

func main() {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	cache := cache_aside.NewCacheAside[*User](client, &User{}, 5*time.Minute)
	result := &User{}
	v, err := cache.Query(ctx, 0, "user:12345", result)
	if err != nil {
		panic(err)
	}
	slog.Info("val", "v", v)
	if result == v {
		slog.Info("result == v")
	} else {
		slog.Info("result != v")
	}
}
