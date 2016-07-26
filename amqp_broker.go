package gocelery

import (
	"encoding/json"
	"log"
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
	exchange         *AMQPExchange
	queue            *AMQPQueue
	consumingChannel <-chan amqp.Delivery
	rate             int
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
	broker := &AMQPCeleryBroker{
		Channel:  NewAMQPConnection(host),
		exchange: NewAMQPExchange("default"),
		queue:    NewAMQPQueue("celery"),
		rate:     4,
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

type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

func NewAMQPExchange(name string) *AMQPExchange {
	return &AMQPExchange{
		Name:       name,
		Type:       "direct",
		Durable:    true,
		AutoDelete: true,
	}
}

func NewAMQPQueue(name string) *AMQPQueue {
	return &AMQPQueue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
	}
}

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
	return nil
}

// GetCeleryMessage hello
func (b *AMQPCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	// connection, exchange, queue already available
	// TODO: timeout feature?
	delivery := <-b.consumingChannel

	// received is task message!!!!

	/*
			   {
			       "expires": null,
		           "utc": true,
		           "args": [5456, 2878],
		           "chord": null,
		           "callbacks": null,
		           "errbacks": null,
		           "taskset": null,
		           "id": "f9bbb86c-4ac2-4816-8ccd-53f904185597",
		           "retries": 0,
		           "task": "worker.add",
		           "timelimit": [null, null],
		           "eta": null, "kwargs": {}
		       }
	*/

	var taskMessage TaskMessage
	json.Unmarshal(delivery.Body, &taskMessage)
	log.Println(taskMessage)
	encoded, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}
	return NewCeleryMessage(encoded), nil
}

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
