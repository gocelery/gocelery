package main

import (
	"context"
	"log"
	"time"

	"github.com/gocelery/gocelery"
)

// add is celery task
func add(a, b int) int {
	return a + b
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	log.Printf("MAIN creating worker...")
	worker, err := gocelery.NewWorker(ctx, &gocelery.WorkerOptions{
		NumWorkers: 1,
	})
	if err != nil {
		log.Fatalf(err.Error())
	}
	// register task
	worker.Register("tasks.add", add)

	log.Printf("MAIN starting worker...")
	worker.Start()
	log.Printf("MAIN sleeping...")
	time.Sleep(10 * time.Second)
	log.Printf("MAIN canceling...")
	cancel()
}
