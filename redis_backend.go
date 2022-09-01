// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackend struct {
	RedisClient redis.UniversalClient
	ctx         *context.Context
}

// NewRedisBackend creates new RedisCeleryBackend with given redis pool.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackend(ctx *context.Context, redisClient redis.UniversalClient) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		RedisClient: redisClient,
		ctx:         ctx,
	}
}

// NewRedisCeleryBackend creates new RedisCeleryBackend
//
// Deprecated: NewRedisCeleryBackend exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
func NewRedisCeleryBackend(ctx *context.Context, uri string) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		RedisClient: NewRedisClient(uri),
		ctx:         ctx,
	}
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	conn := cb.RedisClient
	val, err := conn.Do(*cb.ctx, "GET", fmt.Sprintf("celery-task-meta-%s", taskID)).Result()
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage ResultMessage
	// FIXME::
	cnt := val.(string)
	err = json.Unmarshal([]byte(cnt), &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb *RedisCeleryBackend) SetResult(taskID string, result *ResultMessage) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	conn := cb.RedisClient
	_, err = conn.Do(*cb.ctx, "SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, resBytes).Result()
	return err
}
