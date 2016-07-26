package gocelery

import (
	"encoding/json"
	"log"
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

	log.Println("Getting result from channel")

	delivery := <-channel
	delivery.Ack(false)

	log.Println("GOT result!")

	var resultMessage ResultMessage
	if err := json.Unmarshal(delivery.Body, &resultMessage); err != nil {
		return nil, err
	}

	log.Println(resultMessage)

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
