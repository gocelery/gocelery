package gocelery

import (
	"log"

	"github.com/streadway/amqp"
)

// deliveryAck acknowledges delivery message with retries on error
func deliveryAck(delivery amqp.Delivery) {
	retryCount := 3
	var err error
	for retryCount > 0 {
		if err = delivery.Ack(false); err == nil {
			break
		}
	}
	if err != nil {
		log.Printf("amqp_backend: failed to acknowledge result message %+v: %+v", delivery.MessageId, err)
	}
}
