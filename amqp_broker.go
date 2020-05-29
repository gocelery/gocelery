// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// AMQPExchange stores AMQP Exchange configuration
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPExchange creates new AMQPExchange
func NewAMQPExchange(name string) *AMQPExchange {
	return &AMQPExchange{
		Name:       name,
		Type:       "direct",
		Durable:    true,
		AutoDelete: true,
	}
}

func NewCustomAMQPExchange(name string, exchangeType string) *AMQPExchange {
	return &AMQPExchange{
		Name:       name,
		Type:       exchangeType,
		Durable:    true,
		AutoDelete: false,
	}
}

// AMQPQueue stores AMQP Queue configuration
type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPQueue creates new AMQPQueue
func NewAMQPQueue(name string) *AMQPQueue {
	return &AMQPQueue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
	}
}

//AMQPCeleryBroker is RedisBroker for AMQP
type AMQPCeleryBroker struct {
	*amqp.Channel
	connection       *amqp.Connection
	exchange         *AMQPExchange
	queue            *AMQPQueue
	consumingChannel <-chan amqp.Delivery
	rate             int
}


// NewAMQPConnection creates new AMQP channel
func NewCustomAMQPConnection(host string) (*amqp.Connection, *amqp.Channel, error) {
	connection, err := amqp.Dial(host)
	if err != nil {
		return nil, nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, nil, err
	}
	return connection, channel, nil
}

func NewCustomAMQPCeleryBroker(host string, exchangeName string, exchangeType string, queueName string) (*AMQPCeleryBroker, error) {
	exchange := NewCustomAMQPExchange(exchangeName, exchangeType)
	queue := NewAMQPQueue(queueName)
	conn, channel, err := NewCustomAMQPConnection(host)
	if err != nil {
		return &AMQPCeleryBroker{}, err
	}
	return NewCustomAMQPCeleryBrokerSetup(conn, channel, exchange, queue)
}

func NewCustomAMQPCeleryBrokerSetup(conn *amqp.Connection, channel *amqp.Channel, exchange *AMQPExchange, queue *AMQPQueue) (*AMQPCeleryBroker, error) {
	broker := &AMQPCeleryBroker{
		Channel:    channel,
		connection: conn,
		exchange:   exchange,
		queue:      queue,
		rate:       4,
	}
	if err := broker.CheckExchangeExists(); err != nil {
		return &AMQPCeleryBroker{}, err
	}
	if err := broker.CheckQueueExists(); err != nil {
		return &AMQPCeleryBroker{}, err
	}
	if err := broker.Qos(broker.rate, 0, false); err != nil {
		return &AMQPCeleryBroker{}, err
	}
	if err := broker.StartConsumingChannel(); err != nil {
		return &AMQPCeleryBroker{}, err
	}
	return broker, nil
}


// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return connection, channel
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker
func NewAMQPCeleryBroker(host string) *AMQPCeleryBroker {
	return NewAMQPCeleryBrokerByConnAndChannel(NewAMQPConnection(host))
}

// NewAMQPCeleryBrokerByConnAndChannel creates new AMQPCeleryBroker using AMQP conn and channel
func NewAMQPCeleryBrokerByConnAndChannel(conn *amqp.Connection, channel *amqp.Channel) *AMQPCeleryBroker {
	broker := &AMQPCeleryBroker{
		Channel:    channel,
		connection: conn,
		exchange:   NewAMQPExchange("default"),
		queue:      NewAMQPQueue("celery"),
		rate:       4,
	}
	if err := broker.CreateExchange(); err != nil {
		panic(err)
	}
	if err := broker.CreateQueue(); err != nil {
		panic(err)
	}
	if err := broker.Qos(broker.rate, 0, false); err != nil {
		panic(err)
	}
	if err := broker.StartConsumingChannel(); err != nil {
		panic(err)
	}
	return broker
}

// StartConsumingChannel spawns receiving channel on AMQP queue
func (b *AMQPCeleryBroker) StartConsumingChannel() error {
	channel, err := b.Consume(b.queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.consumingChannel = channel
	return nil
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	taskMessage := message.GetTaskMessage()

	resBytes, err := json.Marshal(taskMessage)
	if err != nil {
		return err
	}

	publishMessage := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}

	return b.Publish(
		b.exchange.Name,
		b.queue.Name,
		false,
		false,
		publishMessage,
	)
}

// GetTaskMessage retrieves task message from AMQP queue
func (b *AMQPCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	select {
	case delivery := <-b.consumingChannel:
		deliveryAck(delivery)
		var taskMessage TaskMessage
		if err := json.Unmarshal(delivery.Body, &taskMessage); err != nil {
			return nil, err
		}
		return &taskMessage, nil
	default:
		return nil, fmt.Errorf("consuming channel is empty")
	}
}

// CreateExchange declares AMQP exchange with stored configuration
func (b *AMQPCeleryBroker) CreateExchange() error {
	return b.ExchangeDeclare(
		b.exchange.Name,
		b.exchange.Type,
		b.exchange.Durable,
		b.exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// CreateQueue declares AMQP Queue with stored configuration
func (b *AMQPCeleryBroker) CreateQueue() error {
	_, err := b.QueueDeclare(
		b.queue.Name,
		b.queue.Durable,
		b.queue.AutoDelete,
		false,
		false,
		nil,
	)
	return err
}

func (b *AMQPCeleryBroker) CheckExchangeExists() error {
	return b.ExchangeDeclarePassive(
		b.exchange.Name,
		b.exchange.Type,
		b.exchange.Durable,
		b.exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// CreateQueue declares AMQP Queue with stored configuration
func (b *AMQPCeleryBroker) CheckQueueExists() error {
	_, err := b.QueueDeclarePassive(
		b.queue.Name,
		b.queue.Durable,
		b.queue.AutoDelete,
		false,
		false,
		nil,
	)
	return err
}