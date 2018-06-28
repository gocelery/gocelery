package gocelery

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

func makeCeleryMessage() (*CeleryMessage, error) {
	taskMessage := getTaskMessage("add")
	taskMessage.Args = []interface{}{rand.Intn(10), rand.Intn(10)}
	defer releaseTaskMessage(taskMessage)
	encodedTaskMessage, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}
	return getCeleryMessage(encodedTaskMessage), nil
}

// test all brokers
func getBrokers() []CeleryBroker {
	return []CeleryBroker{
		NewRedisCeleryBroker("redis://localhost:6379"),
		//NewAMQPCeleryBroker("amqp://"),
	}
}

// TestSend is Redis specific test that sets CeleryMessage to queue
func TestSend(t *testing.T) {
	broker := NewRedisCeleryBroker("redis://localhost:6379")
	celeryMessage, err := makeCeleryMessage()
	if err != nil || celeryMessage == nil {
		t.Errorf("failed to construct celery message: %v", err)
	}
	defer releaseCeleryMessage(celeryMessage)
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

// TestGet is Redis specific test that gets CeleryMessage from queue
func TestGet(t *testing.T) {
	broker := NewRedisCeleryBroker("redis://localhost:6379")
	celeryMessage, err := makeCeleryMessage()
	if err != nil || celeryMessage == nil {
		t.Errorf("failed to construct celery message: %v", err)
	}
	defer releaseCeleryMessage(celeryMessage)
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

// TestSendGet tests set/get features for all brokers
func TestSendGet(t *testing.T) {
	for _, broker := range getBrokers() {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("failed to construct celery message: %v", err)
		}
		defer releaseCeleryMessage(celeryMessage)
		err = broker.SendCeleryMessage(celeryMessage)
		if err != nil {
			t.Errorf("failed to send celery message to broker: %v", err)
		}

		message, err := broker.GetTaskMessage()
		if err != nil {
			t.Errorf("failed to get celery message from broker: %v", err)
		}
		originalMessage := celeryMessage.GetTaskMessage()
		if !reflect.DeepEqual(message, originalMessage) {
			t.Errorf("received message %v different from original message %v", message, originalMessage)
		}
	}
}
