package skydba

import (
	"github.com/redis/go-redis/v9"
	"fmt"
)

type redisCache struct {
	host string
	port int32
	password string
	dbIndex int	
}

func (cache *redisCache) getClient () *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", cache.host, cache.port),
		Password: cache.password,
		DB:      cache.dbIndex,
	})
}

