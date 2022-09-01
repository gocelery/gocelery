// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	RedisClient redis.UniversalClient
	QueueName   string
	ctx         *context.Context
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(ctx *context.Context, redisClient redis.UniversalClient) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		RedisClient: redisClient,
		QueueName:   "celery",
		ctx:         ctx,
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
//
// Deprecated: NewRedisCeleryBroker exists for historical compatibility
// and should not be used. Use NewRedisBroker instead to create new RedisCeleryBroker.
func NewRedisCeleryBroker(ctx *context.Context, uri string) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		RedisClient: NewRedisClient(uri),
		QueueName:   "celery",
		ctx:         ctx,
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.RedisClient

	_, err = conn.Do(*cb.ctx, "LPUSH", cb.QueueName, jsonBytes).Result()
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	conn := cb.RedisClient

	messageJSON, err := conn.Do(*cb.ctx, "BRPOP", cb.QueueName, "1").Result()
	if err != nil {
		return nil, err
	}
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	if string(messageList[0].(string)) != cb.QueueName {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	var message CeleryMessage
	// FIXME::
	cnt := messageList[1].(string)
	if err := json.Unmarshal([]byte(cnt), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}

func NewRedisClient(uri string) redis.UniversalClient {
	opt, _ := redis.ParseURL(uri)
	log.Println(" ------ === opt: ", opt)

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{opt.Addr},
		Username: opt.Username,
		Password: opt.Password,
		DB:       opt.DB,
	})
	return redisClient
}
