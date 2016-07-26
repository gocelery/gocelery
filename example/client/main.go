package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/shicky/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle

func main() {
	// create broker
	celeryBroker := gocelery.NewCeleryRedisBroker("localhost:6379", "")
	//celeryBroker := gocelery.NewAMQPCeleryBroker("amqp://")
	// create backend
	celeryBackend := gocelery.NewCeleryRedisBackend("localhost:6379", "")
	// create client
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, celeryBackend, 0)
	// send task
	asyncResult, err := celeryClient.Delay("worker.add", 3, 5)
	if err != nil {
		panic(err)
	}

	// check if result is ready
	isReady, _ := asyncResult.Ready()
	fmt.Printf("Ready status: %v\n", isReady)

	// get result with 1s timeout
	res, err := asyncResult.Get(1 * time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Result: %v of type: %v\n", res, reflect.TypeOf(res))
	}
}
