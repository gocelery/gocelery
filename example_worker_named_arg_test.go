// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
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

func Example_workerWithNamedArguments() {

	// create redis connection pool
	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
		DB:    0,
	})
	ctx := context.Background()

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(&ctx, redisClient),
		NewRedisBackend(&ctx, redisClient),
		5, // number of workers
	)

	// register task
	cli.Register("add", &exampleAddTask{})

	// start workers (non-blocking call)
	cli.StartWorker()

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()
}
