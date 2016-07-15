package gocelery

import (
	"encoding/base64"
	"encoding/json"
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
