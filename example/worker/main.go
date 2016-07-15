package main

import (
	"time"

	"github.com/shicky/gocelery"
)

// Actual task implemented in Go
func add(a int, b int) int {
	return a + b
}

// Start Celery Worker in Go
func main() {
	celeryBroker := gocelery.NewCeleryRedisBroker("localhost:6379", "")
	// starting with 2 workers
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, 2)
	celeryClient.Register("worker.add", add)
	go celeryClient.StartWorker()
	time.Sleep(30 * time.Second)
	celeryClient.StopWorker()
}
