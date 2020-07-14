// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

// AMQPCeleryBackend CeleryBackend for AMQP
type AMQPCeleryBackend struct {
	*amqp.Channel
	Connection *amqp.Connection
	Host       string
}

// NewAMQPCeleryBackend creates new AMQPCeleryBackend
func NewAMQPCeleryBackend(host string) *AMQPCeleryBackend {
	backend := NewAMQPCeleryBackendByConnAndChannel(NewAMQPConnection(host))
	backend.Host = host
	return backend
}

// NewAMQPCeleryBackendByConnAndChannel creates new AMQPCeleryBackend by AMQP connection and channel
func NewAMQPCeleryBackendByConnAndChannel(conn *amqp.Connection, channel *amqp.Channel) *AMQPCeleryBackend {
	backend := &AMQPCeleryBackend{
		Channel:    channel,
		Connection: conn,
	}
	return backend
}

// Reconnect reconnects to AMQP server
func (b *AMQPCeleryBackend) Reconnect() {
	b.Connection.Close()
	conn, channel := NewAMQPConnection(b.Host)
	b.Channel = channel
	b.Connection = conn
}

// GetResult retrieves result from AMQP queue
func (b *AMQPCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {

	queueName := strings.Replace(taskID, "-", "", -1)

	args := amqp.Table{"x-expires": int32(86400000)}

	_, err := b.QueueDeclare(
		queueName, // name
		true,      // durable
		true,      // autoDelete
		false,     // exclusive
		false,     // noWait
		args,      // args
	)
	if err != nil {
		return nil, err
	}

	err = b.ExchangeDeclare(
		"default",
		"direct",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// open channel temporarily
	channel, err := b.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	var resultMessage ResultMessage

	delivery := <-channel
	deliveryAck(delivery)
	if err := json.Unmarshal(delivery.Body, &resultMessage); err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult sets result back to AMQP queue
func (b *AMQPCeleryBackend) SetResult(taskID string, result *ResultMessage) error {

	result.ID = taskID

	//queueName := taskID
	queueName := strings.Replace(taskID, "-", "", -1)

	// autodelete is automatically set to true by python
	// (406) PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'bc58c0d895c7421eb7cb2b9bbbd8b36f' in vhost '/': received 'true' but current is 'false'

	args := amqp.Table{"x-expires": int32(86400000)}
	_, err := b.QueueDeclare(
		queueName, // name
		true,      // durable
		true,      // autoDelete
		false,     // exclusive
		false,     // noWait
		args,      // args
	)
	if err != nil {
		return err
	}

	err = b.ExchangeDeclare(
		"default",
		"direct",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	message := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}
	return b.Publish(
		"",
		queueName,
		false,
		false,
		message,
	)
}
