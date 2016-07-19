package gocelery

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

func makeCeleryMessage() (*CeleryMessage, error) {
	taskMessage := NewTaskMessage("add", rand.Intn(10), rand.Intn(10))
	encodedTaskMessage, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}
	return NewCeleryMessage(encodedTaskMessage), nil
}

func TestSend(t *testing.T) {
	broker := NewCeleryRedisBroker("localhost:6379", "")
	celeryMessage, err := makeCeleryMessage()
	if err != nil || celeryMessage == nil {
		t.Errorf("failed to construct celery message: %v", err)
	}
	err = broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		t.Errorf("failed to send celery message to broker: %v", err)
	}
	conn := broker.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BLPOP", broker.queueName, "1")
	if err != nil || messageJSON == nil {
		t.Errorf("failed to get celery message from broker: %v", err)
	}
	messageList := messageJSON.([]interface{})
	if string(messageList[0].([]byte)) != "celery" {
		t.Errorf("non celery message received")
	}
	// parse celery message
	var message CeleryMessage
	json.Unmarshal(messageList[1].([]byte), &message)
	if !reflect.DeepEqual(celeryMessage, &message) {
		t.Errorf("received message %v different from original message %v", &message, celeryMessage)
	}
}

func TestGet(t *testing.T) {
	broker := NewCeleryRedisBroker("localhost:6379", "")
	celeryMessage, err := makeCeleryMessage()
	if err != nil || celeryMessage == nil {
		t.Errorf("failed to construct celery message: %v", err)
	}
	jsonBytes, err := json.Marshal(celeryMessage)
	if err != nil {
		t.Errorf("failed to marshal celery message: %v", err)
	}
	conn := broker.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", broker.queueName, jsonBytes)
	if err != nil {
		t.Errorf("failed to push celery message to redis: %v", err)
	}
	// test Get
	message, err := broker.GetCeleryMessage()
	if err != nil {
		t.Errorf("failed to get celery message from broker: %v", err)
	}
	if !reflect.DeepEqual(message, celeryMessage) {
		t.Errorf("received message %v different from original message %v", message, celeryMessage)
	}
}

func TestSendGet(t *testing.T) {
	broker := NewCeleryRedisBroker("localhost:6379", "")
	celeryMessage, err := makeCeleryMessage()
	if err != nil || celeryMessage == nil {
		t.Errorf("failed to construct celery message: %v", err)
	}
	err = broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		t.Errorf("failed to send celery message to broker: %v", err)
	}
	message, err := broker.GetCeleryMessage()
	if err != nil {
		t.Errorf("failed to get celery message from broker: %v", err)
	}
	if !reflect.DeepEqual(message, celeryMessage) {
		t.Errorf("received message %v different from original message %v", message, celeryMessage)
	}
}
