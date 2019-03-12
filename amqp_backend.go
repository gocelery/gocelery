package gocelery

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

type AMQPBackend struct {
	*AMQPConnection
}

// GetResult retrieves result from AMQP queue
func (b *AMQPBackend) GetResult(taskID string) (*ResultMessage, error) {
	queueName := strings.Replace(taskID, "-", "", -1)
	args := amqp.Table{"x-expires": int32(86400000)}
	q, err := b.Channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		args,      // args
	)
	if err != nil {
		return nil, err
	}
	if err = b.Channel.Qos(1, 0, false); err != nil {
		return nil, err
	}
	deliveryChan, err := b.Channel.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	var resultMessage ResultMessage

	select {
	case <-b.CloseErr:
		return nil, ErrConnection
	case <-b.BlockErr:
		return nil, ErrBlocked
	case delivery := <-deliveryChan:
		deliveryAck(delivery)
		if err := json.Unmarshal(delivery.Body, &resultMessage); err != nil {
			return nil, err
		}
		return &resultMessage, nil
	}
}

// SetResult sets result back to AMQP queue
func (b *AMQPBackend) SetResult(taskID string, result *ResultMessage) error {
	result.ID = taskID
	queueName := strings.Replace(taskID, "-", "", -1)
	args := amqp.Table{"x-expires": int32(86400000)}

	// autodelete is automatically set to true by python
	// (406) PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'bc58c0d895c7421eb7cb2b9bbbd8b36f' in vhost '/': received 'true' but current is 'false'
	q, err := b.Channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		args,      // args
	)
	if err != nil {
		return err
	}
	if err = b.Channel.Qos(1, 0, false); err != nil {
		return err
	}
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	select {
	case <-b.CloseErr:
		return ErrConnection
	case <-b.BlockErr:
		return ErrBlocked
	default:
		return b.Channel.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  JSONContentType,
				Body:         resBytes,
			},
		)
	}
}
