package main

import (
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/gocelery/gocelery"
	"github.com/streadway/amqp"
)

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
	ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         []byte("hello"),
		},
	)
}

func main() {

	amqpConnection := gocelery.AMQPConnection{
		ConnStr: "amqp://",
	}

	if err := amqpConnection.Connect(); err != nil {
		panic(err)
	}

	log.Printf("conn finished wtf")
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

	log.Printf("client fucking initia!!!!!")

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	log.Printf("sending task motherfucker!!!!")

	// run task
	asyncResult, err := cli.Delay(taskName, argA, argB)
	if err != nil {
		panic(err)
	}

	// get results from backend with timeout
	res, err := asyncResult.Get(10 * time.Second)
	if err != nil {
		panic(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))

}
