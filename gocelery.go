package main

import (
	"fmt"

	"github.com/satori/go.uuid"

	"gopkg.in/redis.v4"
)

// CeleryMessage is celery-compatible message
type CeleryMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     string                 `json:"eta"`
}

// NewCeleryTask creates new celery task message
func NewCeleryTask(task string) *CeleryMessage {
	return &CeleryMessage{
		ID:   uuid.NewV4().String(),
		Task: task,
	}
}

// CeleryBroker is interface for celery broker
type CeleryBroker interface {
	Send(*CeleryMessage) error
}

// CeleryRedisBroker is Redis implementation of CeleryBroker
type CeleryRedisBroker struct {
	*redis.Client
}

// NewCeleryRedisBroker creates new CeleryRedisBroker
func NewCeleryRedisBroker(addr string, pass string, db int) (*CeleryRedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pass,
		DB:       db,
	})
	// check connection
	pong, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	if pong != "PONG" {
		return nil, fmt.Errorf("failed to ping redis server")
	}
	return &CeleryRedisBroker{client}, nil
}

// Send CeleryMessage to broker
func (cb *CeleryRedisBroker) Send(msg *CeleryMessage) error {
	return nil
}

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker CeleryBroker
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker) (*CeleryClient, error) {
	return &CeleryClient{broker}, nil
}

func main() {
	celeryBroker, _ := NewCeleryRedisBroker("localhost:6379", "", 0)
	celeryClient, _ := NewCeleryClient(celeryBroker)
	_ = celeryClient
}
