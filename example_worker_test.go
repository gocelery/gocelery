// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

func Example_worker() {

	// create redis connection client
	redisClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
		DB:    0,
	})

	ctx := context.Background()
	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(&ctx, redisClient),
		// &RedisCeleryBackend{RedisClient: redisClient},
		NewRedisBackend(&ctx, redisClient),
		5, // number of workers
	)

	// task
	add := func(a, b int) int {
		return a + b
	}

	// register task
	cli.Register("add", add)

	// start workers (non-blocking call)
	cli.StartWorker()

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()

}
