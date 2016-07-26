package gocelery

import (
	"encoding/json"
	"time"

	"github.com/streadway/amqp"
)

//https://abhishek-tiwari.com/post/amqp-rabbitmq-and-celery-a-visual-guide-for-dummies
//http://docs.celeryproject.org/en/latest/userguide/routing.html#amqp-primer

// Exchange - send messages to queue via exchange
// messages are re-routed at exchange to multiple queues

// Queue - message queue

// Bindings - rules that exchange uses to route messages to queues
// optional routing key attribute -

//AMQPCeleryBroker hello
type AMQPCeleryBroker struct {
	*amqp.Channel
	exchange  *AMQPExchange
	queueName string
}

// NewAMQPConnection hello
func NewAMQPConnection(host string) *amqp.Channel {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}
	//defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return channel
}

// NewAMQPCeleryBroker ch
func NewAMQPCeleryBroker(host string) *AMQPCeleryBroker {
	// ensure exchange is initialized
	b := &AMQPCeleryBroker{
		Channel:   NewAMQPConnection(host),
		queueName: "celery",
	}
	err := b.SetExchange("default")
	if err != nil {
		panic(err)
	}
	return b
}

type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

func (b *AMQPCeleryBroker) SetExchange(name string) error {
	e := &AMQPExchange{
		Name:       name,
		Type:       "direct",
		Durable:    true,
		AutoDelete: true,
	}
	err := b.ExchangeDeclare(
		e.Name,
		e.Type,
		e.Durable,
		e.AutoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	b.exchange = e
	return nil
}

func (b *AMQPCeleryBroker) SetQueue(name string) error {
	_, err := b.Channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // autodelete
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	return err
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	return nil
}

// GetCeleryMessage hello
func (b *AMQPCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	return nil, nil
}

func ConsumeExample() error {
	conn, err := amqp.Dial("amqp://")
	if err != nil {
		return err
	}
	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	// declare exchange
	exchangeName := "default"
	err = channel.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil)
	if err != nil {
		return err
	}

	// declare queue
	queueName := "celery"
	_, err = channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	// set quality of service
	rate := 4 // num cpu?
	err = channel.Qos(rate, 0, false)
	if err != nil {
		return err
	}

	// consume
	// result is amqp.Delivery channel
	deliveries, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	// start receiving messages
	for d := range deliveries {
		msg := d.Body
		// do something with message
		_ = msg
	}

	// For result, use amqp.Publishing

	return nil

}

func ResultExample() error {
	conn, err := amqp.Dial("amqp://")
	if err != nil {
		return err
	}
	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	// prepare result message
	message := &ResultMessage{
		Status:    "SUCCESS",
		Result:    1,
		Traceback: nil,
		Children:  nil,
	}
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// TODO: declare queue
	queueName := "celery"
	exchangeName := "default"

	// publish
	amqpMessage := amqp.Publishing{
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Body:            jsonBytes,
	}
	channel.Publish(exchangeName, queueName, false, false, amqpMessage)

	return nil
}

func (b *AMQPCeleryBroker) SendResultMessage() error {
	return nil
}
