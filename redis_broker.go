package gocelery

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisCeleryBroker is CeleryBroker for Redis
type RedisCeleryBroker struct {
	*redis.Pool
	QueueName   string
	stopChannel chan bool
	workWG      sync.WaitGroup
}

type BrokerOptions struct {
	f func(*brokerOptions)
}

type brokerOptions struct {
	QueueName string
}

func BrokerQueueName(queueName string) BrokerOptions {
	return BrokerOptions{func(options *brokerOptions) {
		options.QueueName = queueName
	}}
}

// NewRedisPool creates pool of redis connections
func NewRedisPool(host string, db int, pass string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialDatabase(db))
			if err != nil {
				return nil, err
			}
			if pass != "" {
				if _, err = c.Do("AUTH", pass); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker
func NewRedisCeleryBroker(host string, db int, pass string, options ...BrokerOptions) *RedisCeleryBroker {
	do := brokerOptions{"celery"}
	for _, opt := range options {
		opt.f(&do)
	}
	return &RedisCeleryBroker{
		Pool:      NewRedisPool(host, db, pass),
		QueueName: do.QueueName,
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
	messageJSON, err := conn.Do("BLPOP", cb.QueueName, "1")
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
