package main

import (
	"log"
	"time"

	"github.com/gocelery/gocelery"
	"github.com/streadway/amqp"
)

func add(a, b int) int {
	return a + b
}

func main2() {
	connStr := "amqp://"
	closeErr := make(chan *amqp.Error, 1)
	blockErr := make(chan amqp.Blocking, 1)
	queueName := "celery"

	conn, err := amqp.Dial(connStr)
	if err != nil {
		panic(err)
	}
	conn.NotifyClose(closeErr)
	conn.NotifyBlocked(blockErr)
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	ch.NotifyClose(closeErr)
	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	// todo: exchange
	// todo: qos
	deliveryChan, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	delivery := <-deliveryChan
	log.Printf("received delivery: %s", delivery.Body)
	delivery.Ack(false)
}

func main() {

	amqpConnection := gocelery.AMQPConnection{
		ConnStr: "amqp://localhost:5672",
	}

	if err := amqpConnection.Connect(); err != nil {
		panic(err)
	}

	amqpBroker := gocelery.AMQPBroker{
		AMQPConnection: &amqpConnection,
	}
	amqpBackend := gocelery.AMQPBackend{
		AMQPConnection: &amqpConnection,
	}

	cli, _ := gocelery.NewCeleryClient(
		&amqpBroker,
		&amqpBackend,
		1,
	)

	// register task
	cli.Register("worker.add", add)

	// start workers (non-blocking call)
	cli.StartWorker()

	// wait for client request
	time.Sleep(300 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()

}
