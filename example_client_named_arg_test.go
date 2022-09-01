// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
)

func Example_clientWithNamedArguments() {

	// create redis connection client
	// create redis connection client
	redisClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
		DB:    0,
	})
	ctx := context.Background()
	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(&ctx, redisClient),
		&RedisCeleryBackend{RedisClient: redisClient},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task
	asyncResult, err := cli.DelayKwargs(
		taskName,
		map[string]interface{}{
			"a": argA,
			"b": argB,
		},
	)
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
