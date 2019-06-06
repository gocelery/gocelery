// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Pool
	queueName string
}

// NewRedisPool creates pool of redis connections from given connection string
func NewRedisPool(uri string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(uri)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri,
// queue is an optional parameter to force the queue, default is 'celery'
func NewRedisCeleryBroker(uri string, queue ...string) *RedisCeleryBroker {
	queueName := DefaultQueueName
	if len(queue) > 0 && queue[0] != "" {
		queueName = queue[0]
	}
	return &RedisCeleryBroker{
		Pool:      NewRedisPool(uri),
		queueName: queueName,
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", cb.queueName, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BLPOP", cb.queueName, "1")
	if err != nil {
		return nil, err
	}
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	var message CeleryMessage
	if err := json.Unmarshal(messageList[1].([]byte), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// SetBrokerQueue changes redis broken queue
func (cb *RedisCeleryBroker) SetBrokerQueue(queue string) {
	cb.queueName = queue
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}
