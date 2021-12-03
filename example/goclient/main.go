// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/gocelery/gocelery"
	"github.com/gomodule/redigo/redis"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
func main() {

	// create redis connection pool
	redisPool := &redis.Pool{
		MaxIdle:     3,                 // maximum number of idle connections in the pool
		MaxActive:   0,                 // maximum number of connections allocated by the pool at a given time
		IdleTimeout: 240 * time.Second, // close connections after remaining idle for this duration
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(redisPool, "celery"),
		&gocelery.RedisCeleryBackend{Pool: redisPool},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	taskReflectName := "worker.add_reflect"

	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task
	asyncResult1, err := cli.DelayKwargs(taskName, map[string]interface{}{
		"a": argA,
		"b": argB,
	})
	if err != nil {
		panic(err)
	}
	// get results from backend with timeout
	res1, err := asyncResult1.Get(10 * time.Second)
	if err != nil {
		panic(err)
	}
	log.Printf("kwarg-result: %+v of type %+v", res1, reflect.TypeOf(res1))

	asyncResult2, err := cli.Delay(taskReflectName, argA, argB)
	if err != nil {
		panic(err)
	}
	res2, err := asyncResult2.Get(10 * time.Second)
	if err != nil {
		panic(err)
	}
	log.Printf("reflect-result: %+v of type %+v", res2, reflect.TypeOf(res2))
}
