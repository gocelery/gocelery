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
	connection *amqp.Connection
	exchange   *AMQPExchange
	host       string
}

// NewAMQPCeleryBackend creates new AMQPCeleryBackend
func NewAMQPCeleryBackend(host string) (*AMQPCeleryBackend, error) {
	conn, channel, err := NewAMQPConnection(host)
	if err != nil {
		return nil, err
	}
	// ensure exchange is initialized
	backend := &AMQPCeleryBackend{
		Channel:    channel,
		connection: conn,
		host:       host,
	}
	return backend, nil
}

// Reconnect reconnects to AMQP server
func (b *AMQPCeleryBackend) Reconnect() {
	b.connection.Close()
	conn, channel, err := NewAMQPConnection(b.host)
	if err != nil {
		panic(err)
	}
	b.Channel = channel
	b.connection = conn
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

	// Hack to avoid Exception (503) Reason: "unexpected command received"
	// https://github.com/streadway/amqp/issues/170
	// AMQP does not allow concurrent use of channels!
	// Fixed by implementing periodic polling
	// https://github.com/shicky/gocelery/issues/18
	// time.Sleep(50 * time.Millisecond)

	// open channel temporarily
	channel, err := b.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	var resultMessage ResultMessage

	delivery := <-channel
	delivery.Ack(false)
	if err := json.Unmarshal(delivery.Body, &resultMessage); err != nil {
		return nil, err
	}
	return &resultMessage, nil

	/*
		select {
		case delivery := <-channel:
			delivery.Ack(false)
			if err := json.Unmarshal(delivery.Body, &resultMessage); err != nil {
				return nil, err
			}
			return &resultMessage, nil
		default:
			return nil, fmt.Errorf("failed to read from channel")
		}
	*/
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
