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
	QueueName string
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(conn *redis.Pool) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Pool:      conn,
		QueueName: "celery",
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
//
// Deprecated: NewRedisCeleryBroker exists for historical compatibility
// and should not be used. Use NewRedisBroker instead to create new RedisCeleryBroker.
func NewRedisCeleryBroker(uri string) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Pool:      NewRedisPool(uri),
		QueueName: "celery",
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
	_, err = conn.Do("LPUSH", cb.QueueName, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BRPOP", cb.QueueName, "1")
	if err != nil {
		return nil, err
	}
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	if string(messageList[0].([]byte)) != cb.QueueName {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	var message CeleryMessage
	if err := json.Unmarshal(messageList[1].([]byte), &message); err != nil {
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

// NewRedisPool creates pool of redis connections from given connection string
//
// Deprecated: newRedisPool exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
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
