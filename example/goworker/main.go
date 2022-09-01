// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/gocelery/gocelery"
)

// exampleAddTask is integer addition task
// with named arguments
type exampleAddTask struct {
	a int
	b int
}

func (a *exampleAddTask) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	a.a = int(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	a.b = int(kwargBFloat)
	return nil
}

func (a *exampleAddTask) RunTask() (interface{}, error) {
	result := a.a + a.b
	return result, nil
}

func add(a, b int) int {
	return a + b
}

func main() {

	// create redis connection pool
	ctx := context.Background()

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
		DB:    3,
	})

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(&ctx, redisClient),
		// &gocelery.RedisCeleryBackend{RedisClient: redisClient},
		gocelery.NewRedisBackend(&ctx, redisClient),
		5, // number of workers
	)

	// register task
	cli.Register("worker.add_reflect", &exampleAddTask{})
	cli.Register("worker.add", add)

	// start workers (non-blocking call)
	cli.StartWorker()

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()
}
