// Copyright (c) 2020 met7or.
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackendV2 struct {
	redis.Cmdable
}

// NewRedisBackend creates new RedisCeleryBackend with given redis connection.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackendV2(conn redis.Cmdable) *RedisCeleryBackendV2 {
	return &RedisCeleryBackendV2{
		Cmdable: conn,
	}
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackendV2) GetResult(taskID string) (*ResultMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	defer cancel()

	val, err := cb.Cmdable.Get(ctx, fmt.Sprintf("celery-task-meta-%s", taskID)).Result()
	if err != nil {
		return nil, err
	}

	if val == "" {
		return nil, fmt.Errorf("result not available")
	}

	var resultMessage ResultMessage
	err = json.Unmarshal([]byte(val), &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb *RedisCeleryBackendV2) SetResult(taskID string, result *ResultMessage) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	defer cancel()

	_, err = cb.Cmdable.Set(ctx, fmt.Sprintf("celery-task-meta-%s", taskID), resBytes, 86400).Result()
	return err
}