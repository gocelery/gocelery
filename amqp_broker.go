package gocelery

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"time"
)

const DEFAULT_EXCHANGE_NAME = "default"
const DEFAULT_QUEUE_NAME = "celery"

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
}

// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}
	//defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return connection, channel
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker
func NewAMQPCeleryBroker(host string, exchange *AMQPExchange, queue *AMQPQueue) *AMQPCeleryBroker {
	conn, channel := NewAMQPConnection(host)
	// ensure exchange is initialized
	if exchange == nil {
		exchange = NewAMQPExchange(DEFAULT_EXCHANGE_NAME)
	}
	if queue == nil {
		queue = NewAMQPQueue(DEFAULT_QUEUE_NAME)
	}
	broker := &AMQPCeleryBroker{
		Channel:    channel,
		connection: conn,
		exchange:   exchange,
		queue:      queue,
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
	//log.Printf("sending task ID %s\n", taskMessage.ID)
	queueName := b.queue.Name

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
		"",
		queueName,
		false,
		false,
		publishMessage,
	)
}

// GetTaskMessage retrieves task message from AMQP queue
func (b *AMQPCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	delivery := <-b.consumingChannel
	delivery.Ack(false)
	var taskMessage TaskMessage
	if err := json.Unmarshal(delivery.Body, &taskMessage); err != nil {
		return nil, err
	}
	return &taskMessage, nil
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
