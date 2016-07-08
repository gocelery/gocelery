package gocelery

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
	Body            string                 `json:"body"`
	Headers         map[string]interface{} `json:"headers"`
	ContentType     string                 `json:"content-type"`
	Properties      CeleryProperties       `json:"properties"`
	ContentEncoding string                 `json:"content-encoding"`
}

// CeleryProperties represents properties json
type CeleryProperties struct {
	BodyEncoding  string             `json:"body_encoding"`
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"replay_to"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryTag   string             `json:"delivery_tag"`
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
	Priority   int    `json:"priority"`
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
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
func NewCeleryTask(task string, args ...interface{}) *TaskMessage {
	return &TaskMessage{
		ID:      uuid.NewV4().String(),
		Task:    task,
		Retries: 0,
		Kwargs:  make(map[string]interface{}),
		Args:    args,
		ETA:     time.Now().Format(time.RFC3339),
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

// CeleryBroker is interface for celery broker
type CeleryBroker interface {
	Send(*CeleryMessage) error
	GetResult(string) interface{}
}

// CeleryRedisBroker is Redis implementation of CeleryBroker
type CeleryRedisBroker struct {
	*redis.Client
	queueName string
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
	return &CeleryRedisBroker{client, "celery"}, nil
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

// GetResult calls API to get asynchronous result
// Should be called by AsyncResult
func (cb *CeleryRedisBroker) GetResult(taskID string) interface{} {
	//"celery-task-meta-" + taskID
	return cb.Get(fmt.Sprintf("celery-task-meta-%s", taskID))
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

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := NewCeleryTask(task, args...)
	encodedMessage, err := celeryTask.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := NewCeleryMessage(encodedMessage)
	cc.SendMessage(celeryMessage)
	return &AsyncResult{celeryTask.ID, cc.broker}, nil
}

// AsyncResult is pending result
type AsyncResult struct {
	taskID string
	broker CeleryBroker
}

// Get gets actual result from redis
// TODO: implement retries and timeout feature
func (ar *AsyncResult) Get() interface{} {
	val := ar.broker.GetResult(ar.taskID)
	cmdVal := val.(*redis.StringCmd)
	res, err := cmdVal.Result()
	if err != nil {
		return nil
	}

	var resMap map[string]interface{}

	json.Unmarshal([]byte(res), &resMap)

	if resMap["status"] != "SUCCESS" {
		return nil
	}

	return resMap["result"]
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() bool {
	val := ar.broker.GetResult(ar.taskID)
	cmdVal := val.(*redis.StringCmd)
	_, err := cmdVal.Result()
	if err != nil {
		return false
	}
	return true
}
