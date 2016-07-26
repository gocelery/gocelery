package gocelery

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

type AMQPCeleryBackend struct {
	*amqp.Channel
	exchange *AMQPExchange
}

// NewAMQPCeleryBackend creates new AMQPCeleryBackend
func NewAMQPCeleryBackend(host string) *AMQPCeleryBackend {
	// ensure exchange is initialized
	backend := &AMQPCeleryBackend{
		Channel: NewAMQPConnection(host),
	}
	return backend
}

// GetResult retrieves result from AMQP queue
func (b *AMQPCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	return nil, nil
}

// SetResult sets result back to AMQP queue
func (b *AMQPCeleryBackend) SetResult(taskID string, result *ResultMessage) error {

	result.ID = taskID

	//queueName := taskID
	queueName := strings.Replace(taskID, "-", "", -1)

	_, err := b.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = b.ExchangeDeclare("", "direct", true, false, false, false, nil)
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

// CreateExchange declares AMQP exchange with stored configuration
func (b *AMQPCeleryBackend) CreateExchange() error {
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
