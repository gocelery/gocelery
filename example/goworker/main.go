// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"fmt"
	"time"

	"github.com/gocelery/gocelery"
	"github.com/gomodule/redigo/redis"
)

// exampleAddTask is integer addition task
// with named arguments
type exampleAddTask struct {
}

type vals struct {
	a int
	b int
}

func (_ *exampleAddTask) ParseKwargs(kwargs map[string]interface{}) (interface{}, error) {
	kwargA, ok := kwargs["a"]
	if !ok {
		return nil, fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return nil, fmt.Errorf("malformed kwarg a")
	}
	a := int(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return nil, fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return nil, fmt.Errorf("malformed kwarg b")
	}
	b := int(kwargBFloat)
	result := a + b
	fmt.Printf("kwarg-worker got a=%d, b=%d, with result=%d", a, b, result)
	return &vals{a, b}, nil
}

func (_ *exampleAddTask) RunTask(arg interface{}) (interface{}, error) {
	v := arg.(*vals)
	result := v.a + v.b
	fmt.Printf("kwarg-worker got a=%d, b=%d, with result=%d", v.a, v.b, result)
	return result, nil
}

func Add(a, b int) int {
	result := a + b
	fmt.Printf("reflect-worker got a=%d, b=%d, with result=%d", a, b, result)
	return result
}

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
		5, // number of workers
	)

	// register task
	cli.Register("worker.add", &exampleAddTask{})
	cli.Register("worker.add_reflect", Add)

	// start workers (non-blocking call)
	cli.StartWorker()

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()
}
