package main

import "github.com/streadway/amqp"

func main() {
	conn, err := amqp.Dial("amqp://")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// create channel
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	// declare a queue
	// idempotent -> can be called multiple times
	q, err := ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	// publich message
	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello"),
		},
	)
	if err != nil {
		panic(err)
	}

	// receive message
	msg, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	_ = msg
}
