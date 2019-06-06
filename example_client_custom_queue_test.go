// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"log"
	"math/rand"
	"reflect"
	"time"
)

func Example_clientWithCustomQueue() {

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisCeleryBroker("redis://", "custom_queue"),
		NewRedisCeleryBackend("redis://"),
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task using "custom_queue"
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

	// run task using "another_queue"
	cli.SetBrokerQueue("another_queue")
	asyncResult, err = cli.Delay(taskName, argA, argB)
	if err != nil {
		panic(err)
	}

	// get results from backend with timeout
	res, err = asyncResult.Get(10 * time.Second)
	if err != nil {
		panic(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))

}
