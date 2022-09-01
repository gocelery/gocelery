// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"context"
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/gocelery/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
func main() {

	// create redis connection client

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
		DB:    3,
	})
	ctx := context.Background()

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(&ctx, redisClient),
		gocelery.NewRedisBackend(&ctx, redisClient),
		// &gocelery.RedisCeleryBackend{RedisClient: redisClient},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	// taskName := "worker.add_reflect"
	argA := rand.Intn(10)
	argB := rand.Intn(10)
	log.Println(" a : ", argA, " b : ", argB)
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
	time.Sleep(time.Second * 2)
	//
	asyncResult1, err1 := cli.DelayKwargs(taskName, map[string]interface{}{
		"a": argA + 1,
		"b": argB + 1,
	})

	if err1 != nil {
		panic(err1)
	}

	// get results from backend with timeout
	res1, err1 := asyncResult1.Get(10 * time.Second)
	if err != nil {
		panic(err1)
	}

	log.Printf("result: %+v of type %+v", res1, reflect.TypeOf(res1))

}
