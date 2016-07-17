package gocelery

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	Send(*CeleryMessage) error
	GetTaskMessage() *TaskMessage
}

// CeleryRedisBroker is Redis implementation of CeleryBroker
type CeleryRedisBroker struct {
	*redis.Pool
	queueName   string
	stopChannel chan bool
	workWG      sync.WaitGroup
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
		Pool:      NewRedisPool(host, pass),
		queueName: "celery",
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

	_, err = conn.Do("LPUSH", cb.queueName, jsonBytes)
	if err != nil {
		return err
	}

	return nil
}

// GetTaskMessage retrieve and decode task messages from broker
func (cb *CeleryRedisBroker) GetTaskMessage() *TaskMessage {
	conn := cb.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BLPOP", cb.queueName, "1")
	if err != nil {
		log.Println(err)
	}
	if messageJSON == nil {
		return nil
	}
	messageList := messageJSON.([]interface{})
	// check for celery message
	if string(messageList[0].([]byte)) != "celery" {
		log.Println("not a celery message!")
		return nil
	}

	// parse
	var message CeleryMessage
	json.Unmarshal(messageList[1].([]byte), &message)
	// ensure content-type is 'application/json'
	if message.ContentType != "application/json" {
		log.Println("unsupported content type " + message.ContentType)
		return nil
	}
	// ensure body encoding is base64
	if message.Properties.BodyEncoding != "base64" {
		log.Println("unsupported body encoding " + message.Properties.BodyEncoding)
		return nil
	}
	// ensure content encoding is utf-8
	if message.ContentEncoding != "utf-8" {
		log.Println("unsupported encoding " + message.ContentEncoding)
		return nil
	}
	// decode body
	taskMessage, err := DecodeTaskMessage(message.Body)
	if err != nil {
		log.Println("failed to decode task message")
		return nil
	}

	return taskMessage
}
