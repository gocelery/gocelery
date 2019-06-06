// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"time"
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

// TestBrokerRedisSend is Redis specific test that sets CeleryMessage to queue
func TestBrokerRedisSend(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBroker
	}{
		{
			name:   "send task to redis broker",
			broker: redisBroker,
		},
	}
	for _, tc := range testCases {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		conn := tc.broker.Get()
		defer conn.Close()
		messageJSON, err := conn.Do("BRPOP", tc.broker.routes[defaultRoute].Queue, "1")
		if err != nil || messageJSON == nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		messageList := messageJSON.([]interface{})
		if string(messageList[0].([]byte)) != "celery" {
			t.Errorf("test '%s': non celery message received", tc.name)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		var message CeleryMessage
		if err := json.Unmarshal(messageList[1].([]byte), &message); err != nil {
			t.Errorf("test '%s': failed to unmarshal received message: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(celeryMessage, &message) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, &message, celeryMessage)
		}
		releaseCeleryMessage(celeryMessage)
	}
}

// TestBrokerRedisGet is Redis specific test that gets CeleryMessage from queue
func TestBrokerRedisGet(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBroker
	}{
		{
			name:   "get task from redis broker",
			broker: redisBroker,
		},
	}
	for _, tc := range testCases {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		jsonBytes, err := json.Marshal(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to marshal celery message: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		conn := tc.broker.Get()
		defer conn.Close()
		_, err = conn.Do("LPUSH", tc.broker.routes[defaultRoute].Queue, jsonBytes)
		if err != nil {
			t.Errorf("test '%s': failed to push celery message to redis: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		message, err := tc.broker.GetCeleryMessage()
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(message, celeryMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, celeryMessage)
		}
		releaseCeleryMessage(celeryMessage)
	}
}

// TestBrokerSendGet tests set/get features for all brokers
func TestBrokerSendGet(t *testing.T) {
	testCases := []struct {
		name   string
		broker CeleryBroker
	}{
		{
			name:   "send/get task for redis broker",
			broker: redisBroker,
		},
		{
			name:   "send/get task for amqp broker",
			broker: amqpBroker,
		},
	}
	for _, tc := range testCases {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		// wait arbitrary time for message to propagate
		time.Sleep(1 * time.Second)
		message, err := tc.broker.GetTaskMessage()
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		originalMessage := celeryMessage.GetTaskMessage()
		if !reflect.DeepEqual(message, originalMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, originalMessage)
		}
		releaseCeleryMessage(celeryMessage)
	}
}

func TestAMQPBrokerCustomRoutes(t *testing.T) {
	test := "test amqp broker custom non-default route"

	routedTaskMessage := getTaskMessage("routedTask")
	defer releaseTaskMessage(routedTaskMessage)

	encodedTaskMessage, err := routedTaskMessage.Encode()
	if err != nil {
		t.Errorf("failed to encode routed task message: %s", err)
	}
	celeryMessage := getCeleryMessage(encodedTaskMessage)
	defer releaseCeleryMessage(celeryMessage)

	if err != nil || celeryMessage == nil {
		t.Errorf("test '%s': failed to construct celery message: %v", test, err)
	}

	broker := amqpBroker
	broker.AddRoute("routedTask", &Route{
		Exchange:   "test",
		Queue:      "test",
		RoutingKey: "test",
	}) 

	
	err = broker.SendCeleryMessage(celeryMessage)	
	if err != nil {
		t.Errorf("test '%s': failed to send celery message to broker: %v", test, err)

	}

	// Since the broker can't yet consume from a custom queue, just
	// directly check to make sure the message landed in the correct queue
	_, amqpchannel := NewAMQPConnection("amqp://")
	channel, err := amqpchannel.Consume("test", "", false, false, false, false, nil)
	if err != nil {
		t.Errorf("failed to start consumer on custom queue: %s", err)
	}

	// wait arbitrary time to allow message to propagate and consumer to set up
	time.Sleep(1 * time.Second)

	var taskMessage TaskMessage

	select {
	case delivery := <-channel:
		deliveryAck(delivery)
		if err := json.Unmarshal(delivery.Body, &taskMessage); err != nil {
			t.Errorf("test '%s': failed to get celery message from amqp: %v", test, err)
		}

		originalMessage := celeryMessage.GetTaskMessage()
		if !reflect.DeepEqual(taskMessage, *originalMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", test, taskMessage, originalMessage)
		}

	default:
		t.Errorf("test '%s': failed to retrieve celery message from custom queue: consuming channel empty", test)
	}

	// delete the queue and close the channel used for checking the custom routed message
	err = amqpchannel.Cancel("", true)
	if err != nil {
		t.Errorf("test '%s': failed to close amqp channel: %v", test, err)
	}	

	err = broker.DelRoute("test")
	if err != nil {
		t.Errorf("test '%s': failed to remove stale custom route: %v", test, err)
	}
}

func TestRedisCustomRoute(t *testing.T) {
	test := "test redis broker custom non-default route"

	routedTaskMessage := getTaskMessage("routedTask")
	defer releaseTaskMessage(routedTaskMessage)

	encodedTaskMessage, err := routedTaskMessage.Encode()
	if err != nil {
		t.Errorf("failed to encode routed task message: %s", err)
	}
	celeryMessage := getCeleryMessage(encodedTaskMessage)
	defer releaseCeleryMessage(celeryMessage)

	if err != nil || celeryMessage == nil {
		t.Errorf("test '%s': failed to construct celery message: %v", test, err)
	}

	broker := redisBroker
	broker.AddRoute("routedTask", &Route{
		Exchange:   "test",
		Queue:      "test",
		RoutingKey: "test",
	}) 

	err = broker.SendCeleryMessage(celeryMessage)	
	if err != nil {
		t.Errorf("test '%s': failed to send celery message to broker: %v", test, err)

	}

	conn := broker.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BLPOP", "test", "1")
	if err != nil {
		t.Errorf("test: '%v': failed to BLPOP from Redis queue: %v", test, err)
	}

	if messageJSON == nil {
		t.Errorf("null message received from redis")
	}

	messageList := messageJSON.([]interface{})
	if string(messageList[0].([]byte)) != "test" {
		t.Errorf("test: '%s': invalid message retrieved from redis", test)
	}

	var taskMessage CeleryMessage
	if err := json.Unmarshal(messageList[1].([]byte), &taskMessage); err != nil {
		t.Errorf("test: '%s': failed to unmarshal message: %v", test, err)
	}

	originalMessage := celeryMessage.GetTaskMessage()
	retrievedMessage := taskMessage.GetTaskMessage()
	if !reflect.DeepEqual(retrievedMessage, originalMessage) {
		t.Errorf("test: '%s': received message %v different from original message %v", test, retrievedMessage, originalMessage)
	}

	err = broker.DelRoute("test")
	if err != nil {
		t.Errorf("test: '%s': failed to remove stale custom queue from redis: %v", test, err)
	}
}

// TestBrokerDefaultRouteChange tests set/get features for all brokers
func TestBrokerDefaultRouteChange(t *testing.T) {
	testCases := []struct {
		name   string
		broker CeleryBroker
	}{
		{
			name:   "send/get task with changed default route for redis broker",
			broker: redisBroker,
		},
		{
			name:   "send/get task with changed default route for amqp broker",
			broker: amqpBroker,
		},
	}
	for _, tc := range testCases {
		tc.broker.SetDefaultRoute(&Route{
			Exchange:   "newExchange",
			RoutingKey: "newQueue",
			Queue:      "newQueue",
		})

		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		// wait arbitrary time for message to propagate
		time.Sleep(1 * time.Second)
		message, err := tc.broker.GetTaskMessage()
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		originalMessage := celeryMessage.GetTaskMessage()
		if !reflect.DeepEqual(message, originalMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, originalMessage)
		}
		releaseCeleryMessage(celeryMessage)
		// make sure to set the broker's default route back to normal after test is complete
		tc.broker.SetDefaultRoute(&Route{
			Exchange:   defaultExchangeName,
			RoutingKey: defaultQueueName,
			Queue:      defaultQueueName,
		})
	}
}
