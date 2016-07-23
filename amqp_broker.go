package gocelery

import "github.com/streadway/amqp"

//AMQPCeleryBroker hello
type AMQPCeleryBroker struct {
	*amqp.Channel
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
	return &AMQPCeleryBroker{
		Channel:   NewAMQPConnection(host),
		queueName: "celery",
	}
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(msg *CeleryMessage) error {
	return nil
}

// GetCeleryMessage hello
func (b *AMQPCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	return nil, nil
}
