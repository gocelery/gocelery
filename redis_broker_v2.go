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

type RedisCeleryBrokerV2 struct {
	redis.Cmdable
	QueueName string
}

// NewRedisBrokerV2 creates new RedisCeleryBroker with given redis connection
func NewRedisBrokerV2(conn redis.Cmdable) *RedisCeleryBrokerV2 {
	return &RedisCeleryBrokerV2{
		Cmdable:   conn,
		QueueName: "celery",
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBrokerV2) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	defer cancel()

	_, err = cb.Cmdable.LPush(ctx, cb.QueueName, jsonBytes).Result()
	if err != nil {
		return err
	}

	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBrokerV2) GetCeleryMessage() (*CeleryMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	defer cancel()

	arr, err := cb.Cmdable.BRPop(ctx, time.Second, cb.QueueName).Result()
	if err != nil {
		return nil, err
	}

	if len(arr) < 2 {
		return nil, fmt.Errorf("null message received from redis")
	}

	if arr[0] != "celery" {
		return nil, fmt.Errorf("not a celery message: %v", arr[0])
	}

	var message CeleryMessage
	if err := json.Unmarshal([]byte(arr[1]), &message); err != nil {
		return nil, err
	}

	return &message, nil
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBrokerV2) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}
