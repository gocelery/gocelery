package gocelery

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBroker is CeleryBroker for Redis
type RedisCeleryBroker struct {
	*redis.Pool
	queueName   string
	stopChannel chan bool
	workWG      sync.WaitGroup
}

// NewRedisPool creates pool of redis connections from given uri
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
		Pool:      NewRedisPool(uri),
		queueName: "celery",
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
	// check for celery message
	if string(messageList[0].([]byte)) != "celery" {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	// parse
	var message CeleryMessage
	json.Unmarshal(messageList[1].([]byte), &message)
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
