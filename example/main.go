package main

import (
	"fmt"
	"time"

	"github.com/shicky/gocelery"
)

func main() {
	celeryBroker, _ := gocelery.NewCeleryRedisBroker("localhost:6379", "", 0)
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker)
	asyncResult, err := celeryClient.Delay("worker.add", 3, 2)
	if err != nil {
		panic(err)
	}
	isReady := asyncResult.Ready()
	for isReady == false {
		isReady = asyncResult.Ready()
		time.Sleep(2 * time.Second)
	}

	res := asyncResult.Get()

	fmt.Println(res)

}
