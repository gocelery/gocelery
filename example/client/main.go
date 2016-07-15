package main

import (
	"fmt"
	"time"

	"github.com/shicky/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle

func main() {
	celeryBroker := gocelery.NewCeleryRedisBroker("localhost:6379", "")
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, 0)
	asyncResult, err := celeryClient.Delay("worker.add", 3, 5)
	if err != nil {
		panic(err)
	}
	isReady, _ := asyncResult.Ready()
	for isReady == false {
		isReady, _ = asyncResult.Ready()
		time.Sleep(2 * time.Second)
	}

	res, _ := asyncResult.Get()

	fmt.Println(res)
}
