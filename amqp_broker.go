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
	routes           map[string]*Route
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
		exchange:   NewAMQPExchange(defaultExchangeName),
		queue:      NewAMQPQueue(defaultQueueName),
		rate:       4,
		routes: map[string]*Route{defaultRoute: &Route{
			Exchange:   defaultExchangeName,
			RoutingKey: defaultQueueName,
			Queue:      defaultQueueName}},
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

// AddRoute defines an exchange -> routing key -> queue Route for a task with a given name
func (b *AMQPCeleryBroker) AddRoute(taskName string, route *Route) {
	b.routes[taskName] = route
}

// GetRoute returns a custom route for the specified task if one is defined, otherwise the default route
func (b *AMQPCeleryBroker) GetRoute(taskName string) *Route {
	route, ok := b.routes[taskName]
	if !ok {
		return b.routes[defaultRoute]
	}

	return route
}

// DelRoute removes a specified route from the local map as well as deleting the Queue on amqp
// but only if there are no consumers on its channel
func (b AMQPCeleryBroker) DelRoute(taskName string) error {
	delete(b.routes, taskName)
	_, err := b.QueueDelete(taskName, false, true, true)

	if err != nil {
		return err
	}

	return nil
}

// SetDefaultRoute modifies the default route
func (b *AMQPCeleryBroker) SetDefaultRoute(route *Route) {
	// delete the old queue if there are no consumers
	_, err := b.QueueDelete(b.routes[defaultRoute].Queue, false, true, true)
	if err != nil {
		panic(err)
	}

	b.routes[defaultRoute] = route

	// When the default route changes we change the queue that the consuming channel listens on
	if err = b.ConfigureAMQPServerRoute(route); err != nil {
		panic(err)
	}
	b.queue.Name = route.Queue
	if err := b.StartConsumingChannel(); err != nil {
		panic(err)
	}
}

// ConfigureAMQPServerRoute declares the Queue and Exchange from a route, then binds them using the routing key
func (b *AMQPCeleryBroker) ConfigureAMQPServerRoute(route *Route) error {
	_, err := b.QueueDeclare(
		route.Queue, // name
		true,        // durable
		false,       // autoDelete
		false,       // exclusive
		false,       // noWait
		nil,         // args
	)
	if err != nil {
		return err
	}

	err = b.ExchangeDeclare(
		route.Exchange,
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

	err = b.QueueBind(
		route.Queue,
		route.RoutingKey,
		route.Exchange,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
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

	route := b.GetRoute(taskMessage.Task)

	err := b.ConfigureAMQPServerRoute(route)
	if err != nil {
		return err
	}

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
		route.Exchange,
		route.RoutingKey,
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
