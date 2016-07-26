package gocelery

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"reflect"
	"time"

	"github.com/satori/go.uuid"
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

// GetTaskMessage retrieve and decode task messages from broker
func (message *CeleryMessage) GetTaskMessage() *TaskMessage {
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

// TaskMessage is celery-compatible message
type TaskMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     string                 `json:"eta"`
}

// NewTaskMessage creates new celery task message
func NewTaskMessage(task string, args ...interface{}) *TaskMessage {
	return &TaskMessage{
		ID:      uuid.NewV4().String(),
		Task:    task,
		Retries: 0,
		Kwargs:  make(map[string]interface{}),
		Args:    args,
		ETA:     time.Now().Format(time.RFC3339),
	}
}

// DecodeTaskMessage decodes base64 encrypted body and return TaskMessage object
func DecodeTaskMessage(encodedBody string) (*TaskMessage, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}

	//log.Println(string(body))

	var message TaskMessage
	err = json.Unmarshal(body, &message)
	if err != nil {
		return nil, err
	}
	return &message, nil

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

// ResultMessage is return message received from broker
type ResultMessage struct {
	ID        string        `json:"task_id"`
	Status    string        `json:"status"`
	Traceback interface{}   `json:"traceback"`
	Result    interface{}   `json:"result"`
	Children  []interface{} `json:"children"`
}

// NewResultMessage returns valid celery result message from result
func NewResultMessage(val reflect.Value) *ResultMessage {
	return &ResultMessage{
		Status:    "SUCCESS",
		Traceback: nil,
		Result:    GetRealValue(val),
		Children:  nil,
	}
}
