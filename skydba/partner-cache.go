package skydba

import (
	"encoding/json"
	"context"
)

type Partner struct {
	Id int64 `json:"id"`
	LastName string `json:"lastName"`
	MiddleName string `json:"middleName"`
	FirstName string `json:"firstName"`
}

type PartnerCache interface {
	Set (key string, value *Partner) error
	Get (key string) (*Partner, error)
}

func NewPartnerCache (host string, port int32, password string, dbIndex int) PartnerCache {
	return &redisCache {
		host: host,
		port: port,
		password: password,
		dbIndex: dbIndex,
	}
}

func (cache *redisCache) Set (key string, value *Partner) error {
	json, err := json.Marshal(value)

	if err != nil {
		return err
	}

	client := cache.getClient()
	ctx := context.Background()
	err = client.Set(ctx, key, json, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (cache *redisCache) Get (key string) (*Partner, error) {
	client := cache.getClient()
	ctx := context.Background()
	val, err := client.Get(ctx, key).Result()

	if err != nil {
		return nil, err
	}

	partner := Partner{}
	err = json.Unmarshal([]byte(val), &partner)
	if err != nil {
		return nil, err
	}

	return &partner, nil
}