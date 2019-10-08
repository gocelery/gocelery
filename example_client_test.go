// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
)

func Example_client() {

	// create redis connection pool
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://")
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(redisPool),
		&RedisCeleryBackend{Pool: redisPool},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

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
