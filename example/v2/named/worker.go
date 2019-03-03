package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocelery/gocelery"
)

// add is celery task
func add(a, b int) int {
	return a + b
}

// AddTask is celery task with named arguments
type AddTask struct {
	a int
	b int
}

// Parse parses named arguments
// int is always float64 for json parsing
func (a *AddTask) Parse(kwargs map[string]interface{}) error {
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

// Run executes celery task with named arguments
func (a *AddTask) Run() (interface{}, error) {
	return a.a + a.b, nil
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
	worker.Register("tasksv2.add", add)

	log.Printf("MAIN starting worker...")
	worker.Start()
	log.Printf("MAIN sleeping...")
	time.Sleep(10 * time.Second)
	log.Printf("MAIN canceling...")
	cancel()
}
