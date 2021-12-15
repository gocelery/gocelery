// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const exchangeName = "celery-exchange"

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
		Type:       "topic",
		Durable:    true,
		AutoDelete: true,
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

//AMQPCeleryBroker is Broker client for AMQP
type AMQPCeleryBroker struct {
	connection *amqp.Connection

	exchange      *AMQPExchange
	queue         *AMQPQueue
	prefetchCount int
	listener      <-chan amqp.Delivery
	connect       func() error
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker using AMQP conn and channel
func NewAMQPCeleryBroker(host string, config *amqp.Config, queueName string) (*AMQPCeleryBroker, error) {
	broker := &AMQPCeleryBroker{
		exchange:      NewAMQPExchange(exchangeName),
		queue:         NewAMQPQueue(queueName),
		prefetchCount: 4,
	}
	connMutex := new(sync.Mutex)
	broker.connect = func() error {
		connMutex.Lock()
		defer connMutex.Unlock()
		return broker.lazyConnect(host, config)
	}
	return broker, nil
}

// Listen spawns receiving channel on AMQP queue
func (b *AMQPCeleryBroker) Listen() error {
	channel, err := b.openChannel()
	if err != nil {
		return err
	}
	b.listener, err = channel.Consume(b.queue.Name, "", false, false, false, true, nil)
	if err != nil {
		return err
	}
	return nil
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage, _ ...string) error {
	channel, err := b.openChannel()
	if err != nil {
		return err
	}
	defer func() {
		err = channel.Close()
		if err != nil {
			fmt.Println("error closing channel")
		}
	}()
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
	return channel.Publish(
		exchangeName,
		b.queue.Name,
		false,
		false,
		publishMessage,
	)
}

// GetTaskMessage retrieves task message from AMQP queue
func (b *AMQPCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	select {
	case delivery := <-b.listener:
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

// createExchange declares AMQP exchange with stored configuration
func createExchange(exchange *AMQPExchange, channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Durable,
		exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// createQueue declares AMQP Queue with stored configuration
func createQueue(queue *AMQPQueue, channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = channel.QueueBind(
		queue.Name,
		"#",
		exchangeName,
		false,
		nil,
	)
	return err
}

func (b *AMQPCeleryBroker) openChannel() (*amqp.Channel, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}
	channel, err := b.connection.Channel()
	if err != nil {
		return nil, err
	}
	if err = createExchange(b.exchange, channel); err != nil {
		return nil, err
	}
	if err = createQueue(b.queue, channel); err != nil {
		return nil, err
	}
	if err = channel.Qos(b.prefetchCount, 0, false); err != nil {
		return nil, err
	}
	return channel, nil
}

// lazyConnect connects to rabbitmq and handles recovery
func (b *AMQPCeleryBroker) lazyConnect(host string, config *amqp.Config) error {
	if b.connection == nil || b.connection.IsClosed() {
		connection, err := amqp.DialConfig(host, *config)
		if err != nil {
			return err
		}
		b.connection = connection
		if b.listener != nil {
			// implies that we were previously subscribed but lost connection, so restore it
			if err = b.Listen(); err != nil {
				return err
			}
		}
	}
	return nil
}
