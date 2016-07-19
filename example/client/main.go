package main

import (
	"fmt"
	"time"

	"github.com/shicky/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle

func main() {
	// create broker
	celeryBroker := gocelery.NewCeleryRedisBroker("localhost:6379", "")
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
	fmt.Printf("ready status %v\n", isReady)

	// get result with 5s timeout
	res, err := asyncResult.Get(5 * time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}
}
