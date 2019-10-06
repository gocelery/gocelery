package gocelery

import (
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// AMQPBroker is broker for amqp protocol
// user can supply own version of Connection or Channel
// if not, they are initialized
type AMQPBroker struct {
	*AMQPConnection
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPBroker) SendCeleryMessage(message *CeleryMessage) error {
	taskMessage := message.GetTaskMessage()
	body, err := json.Marshal(taskMessage)
	if err != nil {
		return err
	}
	// check connection
	if err := b.Connect(); err != nil {
		return err
	}
	// queue declare
	q, err := b.Channel.QueueDeclare(
		b.getQueueName(),
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	// publish
	select {
	case <-b.CloseErr:
		return ErrConnection
	case <-b.BlockErr:
		return ErrBlocked
	default:
		log.Printf("publishing to channel %s", q.Name)
		return b.Channel.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  JSONContentType,
				Body:         body,
			},
		)
	}
}

// GetTaskMessage is blocking for amqp
func (b *AMQPBroker) GetTaskMessage() (*TaskMessage, error) {

	// check connection
	if err := b.Connect(); err != nil {
		return nil, err
	}

	q, err := b.Channel.QueueDeclare(
		b.getQueueName(),
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	// todo: qos

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

	select {
	case <-b.CloseErr:
		return nil, ErrConnection
	case <-b.BlockErr:
		return nil, ErrBlocked
	case delivery := <-deliveryChan:
		var taskMessage TaskMessage
		deliveryAck(delivery)
		log.Printf("%s", delivery.Body)
		if err := json.Unmarshal(delivery.Body, &taskMessage); err != nil {
			return nil, err
		}
		log.Printf("task: %+v", taskMessage)
		return &taskMessage, nil
	}
}
