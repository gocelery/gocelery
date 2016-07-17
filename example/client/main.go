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

	// wait until result is ready
	isReady, _ := asyncResult.Ready()
	for isReady == false {
		isReady, _ = asyncResult.Ready()
		time.Sleep(2 * time.Second)
	}

	// get the result
	res, _ := asyncResult.Get()

	fmt.Println(res)
}
