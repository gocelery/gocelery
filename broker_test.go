// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
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

// stand-alone/cluster redis broker send
func TestBrokerRedisV2Send(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBrokerV2
	}{
		{
			name:   "send task to redis stand-alone broker",
			broker: redisBrokerV2,
		},
		{
			name: "send task to redis cluster broker",
			broker: redisBrokerV2Cluster,
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

		ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
		arr, err := tc.broker.BRPop(ctx, time.Second, tc.broker.QueueName).Result()
		if err != nil || len(arr) < 0 {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}

		if arr[0] != "celery" {
			t.Errorf("test '%s': non celery message received", tc.name)
			releaseCeleryMessage(celeryMessage)
			continue
		}

		var message CeleryMessage
		if err := json.Unmarshal([]byte(arr[1]), &message); err != nil {
			t.Errorf("test '%s': failed to unmarshal received message: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(celeryMessage, &message) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, &message, celeryMessage)
		}

		cancel()
		releaseCeleryMessage(celeryMessage)
	}
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
		{
			name:   "send task to redis broker with connection",
			broker: redisBrokerWithConn,
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
		messageJSON, err := conn.Do("BRPOP", tc.broker.QueueName, "1")
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
		conn.Close()
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
		{
			name:   "get task from redis broker with connection",
			broker: redisBrokerWithConn,
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
		_, err = conn.Do("LPUSH", tc.broker.QueueName, jsonBytes)
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
		conn.Close()
		releaseCeleryMessage(celeryMessage)
	}
}

// stand-alone/cluster redis broker get
func TestBrokerRedisV2Get(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBrokerV2
	}{
		{
			name:   "send task to redis stand-alone broker",
			broker: redisBrokerV2,
		},
		{
			name: "send task to redis cluster broker",
			broker: redisBrokerV2Cluster,
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

		ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
		_, err = tc.broker.LPush(ctx, tc.broker.QueueName, jsonBytes).Result()
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

		cancel()
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
			name:   "send/get task for redis broker with connection",
			broker: redisBrokerWithConn,
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
