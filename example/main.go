package main

import (
	"fmt"
	"time"

	"github.com/shicky/gocelery"
)

func main() {
	celeryBroker := gocelery.NewCeleryRedisBroker("localhost:6379", "")
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker)
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
