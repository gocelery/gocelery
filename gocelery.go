package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/satori/go.uuid"

	"gopkg.in/redis.v4"
)

// CeleryMessage is actual message to be sent to Redis
type CeleryMessage struct {
	Body            string
	Headers         map[string]interface{}
	ContentType     string
	Properties      CeleryProperties
	ContentEncoding string
}

// CeleryProperties represents properties json
type CeleryProperties struct {
	BodyEncoding  string
	CorrelationID string
	ReplyTo       string
	DeliveryInfo  CeleryDeliveryInfo
	DeliveryMode  int
	DeliveryTag   string
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
	Priority   int
	RoutingKey string
	Exchange   string
}

// NewCeleryMessage creates new celery message from encoded task message
func NewCeleryMessage(encodedTaskMessage string) *CeleryMessage {
	var headers map[string]interface{}
	var properties = CeleryProperties{
		BodyEncoding:  "base64",
		CorrelationID: uuid.NewV4().String(),
		ReplyTo:       uuid.NewV4().String(),
		DeliveryInfo: CeleryDeliveryInfo{
			Priority:   0,
			RoutingKey: "celery",
			Exchange:   "celery",
		},
		DeliveryMode: 2,
		DeliveryTag:  uuid.NewV4().String(),
	}
	return &CeleryMessage{
		Body:            encodedTaskMessage,
		Headers:         headers,
		ContentType:     "application/json",
		Properties:      properties,
		ContentEncoding: "utf-8",
	}
}

// TaskMessage is celery-compatible message
type TaskMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     string                 `json:"eta"`
}

// NewCeleryTask creates new celery task message
func NewCeleryTask(task string) *TaskMessage {
	var args []interface{}
	var kwargs map[string]interface{}
	return &TaskMessage{
		ID:      uuid.NewV4().String(),
		Task:    task,
		Retries: 0,
		Kwargs:  kwargs,
		Args:    args,
		ETA:     time.Now().String(),
	}
}

// Encode returns base64 json encoded string
func (tm *TaskMessage) Encode() (string, error) {
	jsonData, err := json.Marshal(tm)
	if err != nil {
		return "", err
	}
	encodedData := base64.StdEncoding.EncodeToString(jsonData)
	return encodedData, err
}

/*
"id": "c8535050-68f1-4e18-9f32-f52f1aab6d9b",
"task": "worker.add",
"args": [5456, 2878],
"kwargs": {}
"retries": 0,
"eta": null,

"expires": null,
"utc": true,
"chord": null,
"callbacks": null,
"errbacks": null,
"taskset": null,
"timelimit": [null, null],
*/

// CeleryBroker is interface for celery broker
type CeleryBroker interface {
	Send(*CeleryMessage) error
}

// CeleryRedisBroker is Redis implementation of CeleryBroker
type CeleryRedisBroker struct {
	*redis.Client
	queueName string
}

// NewCeleryRedisBroker creates new CeleryRedisBroker
func NewCeleryRedisBroker(queueName string, addr string, pass string, db int) (*CeleryRedisBroker, error) {
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
	return &CeleryRedisBroker{client, queueName}, nil
}

// Send CeleryMessage to broker
func (cb *CeleryRedisBroker) Send(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	cb.LPush(cb.queueName, jsonBytes)
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

// SendMessage sends celery task message to broker
func (cc *CeleryClient) SendMessage(message *CeleryMessage) error {
	cc.broker.Send(message)
	return nil
}

func main() {
	celeryBroker, _ := NewCeleryRedisBroker("tasks", "localhost:6379", "", 0)
	celeryClient, _ := NewCeleryClient(celeryBroker)

	// prepare task message
	celeryTask := NewCeleryTask("worker.add")
	celeryTask.Kwargs = make(map[string]interface{})
	args := []interface{}{4, 5}
	celeryTask.Args = append(celeryTask.Args, args...)
	encodedBody, err := celeryTask.Encode()
	if err != nil {
		panic(err)
	}
	celeryMessage := NewCeleryMessage(encodedBody)
	celeryClient.SendMessage(celeryMessage)
}
