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
	routes map[string]*Route
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

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
func NewRedisCeleryBroker(uri string) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Pool: NewRedisPool(uri),
		routes: map[string]*Route{defaultRoute: &Route{
			Exchange:   "", // Redis broker doesn't use exhanges or routing keys, only queue names
			RoutingKey: "",
			Queue:      defaultQueueName,
		}},
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	taskMessage := message.GetTaskMessage()
	route := cb.GetRoute(taskMessage.Task)

	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", route.Queue, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BRPOP", cb.routes[defaultRoute].Queue, "1")
	if err != nil {
		return nil, err
	}
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	if string(messageList[0].([]byte)) != cb.routes[defaultRoute].Queue {
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

// AddRoute defines the destination queue for a task with a given name
func (cb *RedisCeleryBroker) AddRoute(taskName string, route *Route) {
	cb.routes[taskName] = route
}

// GetRoute returns a custom route for the specified task if one is defined, otherwise the default route
func (cb *RedisCeleryBroker) GetRoute(taskName string) *Route {
	route, ok := cb.routes[taskName]
	if !ok {
		return cb.routes[defaultRoute]
	}

	return route
}

// SetDefaultRoute modifies the default route
func (cb *RedisCeleryBroker) SetDefaultRoute(route *Route) {
	cb.routes[defaultRoute] = route
}

// DelRoute removes a specified route from the the local map and the key list from the redis server
func (cb *RedisCeleryBroker) DelRoute(taskName string) error {
	delete(cb.routes, taskName)

	conn := cb.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", taskName)
	if err != nil {
		return err
	}

	return nil
}
