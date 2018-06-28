package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/gocelery/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle

func main() {

	// create broker and backend
	celeryBroker := gocelery.NewRedisCeleryBroker("redis://localhost:6379")
	celeryBackend := gocelery.NewRedisCeleryBackend("redis://localhost:6379")

	// AMQP example
	//celeryBroker := gocelery.NewAMQPCeleryBroker("amqp://")
	//celeryBackend := gocelery.NewAMQPCeleryBackend("amqp://")

	// create client
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, celeryBackend, 0)

	arg1 := rand.Intn(10)
	arg2 := rand.Intn(10)

	asyncResult, err := celeryClient.Delay("worker.add", arg1, arg2)
	if err != nil {
		panic(err)
	}

	res, err := asyncResult.Get(10 * time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Result: %v of type: %v\n", res, reflect.TypeOf(res))
	}

	// send task
	/*
		asyncResult, err = celeryClient.DelayKwargs("worker.add_reflect", map[string]interface{}{
			"x": 3,
			"y": 5,
		})
		if err != nil {
			panic(err)
		}

		// check if result is ready
		isReady, _ := asyncResult.Ready()
		fmt.Printf("Ready status: %v\n", isReady)

		// get result with 1s timeout
		res2, err := asyncResult.Get(10 * time.Second)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("Result: %v of type: %v\n", res2, reflect.TypeOf(res2))
		}
	*/
}
