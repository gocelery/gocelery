package gocelery

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

// CeleryBroker is interface for celery broker
type CeleryBroker interface {
	Send(*CeleryMessage) error
	GetResult(string) (interface{}, error)
}

// CeleryRedisBroker is Redis implementation of CeleryBroker
type CeleryRedisBroker struct {
	*redis.Pool
	queueName string
}

// NewRedisPool creates pool of redis connections
func NewRedisPool(host, pass string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
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

// NewCeleryRedisBroker creates new CeleryRedisBroker
func NewCeleryRedisBroker(host, pass string) *CeleryRedisBroker {
	return &CeleryRedisBroker{
		NewRedisPool(host, pass),
		"celery",
	}
}

// Send CeleryMessage to broker
func (cb *CeleryRedisBroker) Send(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()

	val, err := conn.Do("LPUSH", cb.queueName, jsonBytes)
	log.Printf("val %v err %v\n", val, err)

	if err != nil {
		return err
	}

	return nil
}

// GetResult calls API to get asynchronous result
// Should be called by AsyncResult
func (cb *CeleryRedisBroker) GetResult(taskID string) (interface{}, error) {
	//"celery-task-meta-" + taskID
	conn := cb.Get()
	defer conn.Close()
	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
	if err != nil {
		return nil, err
	}
	return val, nil
}
