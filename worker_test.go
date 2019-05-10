package gocelery

import (
	"math/rand"
	"testing"
	"time"
)

// add is test task method
func add(a int, b int) int {
	return a + b
}

func addFloat(a float32, b float32) float32 {
	return a + b
}

// newCeleryWorker creates celery worker
func newCeleryWorker(numWorkers int) *CeleryWorker {
	broker := NewRedisCeleryBroker("redis://localhost:6379")
	backend := NewRedisCeleryBackend("redis://localhost:6379")
	celeryWorker := NewCeleryWorker(broker, backend, numWorkers)
	return celeryWorker
}

// registerTask registers add test task
func registerTask(celeryWorker *CeleryWorker) string {
	taskName := "add"
	registeredTask := add
	celeryWorker.Register(taskName, registeredTask)
	return taskName
}

func registerAddFloat(celeryWorker *CeleryWorker) string {
	taskName := "addFloat"
	registeredTask := addFloat
	celeryWorker.Register(taskName, registeredTask)
	return taskName
}

func TestRegisterTask(t *testing.T) {
	celeryWorker := newCeleryWorker(1)
	taskName := registerTask(celeryWorker)
	receivedTask := celeryWorker.GetTask(taskName)
	if receivedTask == nil {
		t.Errorf("failed to retrieve task")
	}
}

func TestRunTask(t *testing.T) {
	celeryWorker := newCeleryWorker(1)
	taskName := registerTask(celeryWorker)

	// prepare args
	args := []interface{}{
		rand.Int(),
		rand.Int(),
	}

	// Run task normally
	res := add(args[0].(int), args[1].(int))

	// construct task message
	taskMessage := &TaskMessage{
		ID:      generateUUID(),
		Task:    taskName,
		Args:    args,
		Kwargs:  nil,
		Retries: 1,
		ETA:     "",
	}
	resultMsg, err := celeryWorker.RunTask(taskMessage)
	if err != nil {
		t.Errorf("failed to run celery task %v: %v", taskMessage, err)
	}

	reflectRes := resultMsg.Result.(int64)

	// check result
	if int64(res) != reflectRes {
		t.Errorf("reflect result %v is different from normal result %v", reflectRes, res)
	}
}

func TestNumWorkers(t *testing.T) {
	numWorkers := rand.Intn(10)
	celeryWorker := newCeleryWorker(numWorkers)
	celeryNumWorkers := celeryWorker.GetNumWorkers()
	if numWorkers != celeryNumWorkers {
		t.Errorf("number of workers are different: %d vs %d", numWorkers, celeryNumWorkers)
	}
}

func TestStartStop(t *testing.T) {
	numWorkers := rand.Intn(10)
	celeryWorker := newCeleryWorker(numWorkers)
	_ = registerTask(celeryWorker)
	go celeryWorker.StartWorker()
	time.Sleep(100 * time.Millisecond)
	celeryWorker.StopWorker()
}

func TestRunTaskWithFloat(t *testing.T) {
	celeryWorker := newCeleryWorker(1)
	taskName := registerAddFloat(celeryWorker)

	// prepare args
	args := []interface{}{
		rand.Float32(),
		rand.Float32(),
	}

	// Run task normally
	res := addFloat(args[0].(float32), args[1].(float32))
	// construct task message
	taskMessage := &TaskMessage{
		ID:      generateUUID(),
		Task:    taskName,
		Args:    args,
		Kwargs:  nil,
		Retries: 1,
		ETA:     "",
	}
	resultMsg, err := celeryWorker.RunTask(taskMessage)
	if err != nil {
		t.Errorf("failed to run celery task %v: %v", taskMessage, err)
	}

	reflectRes := float32(resultMsg.Result.(float64))

	// check result
	if float32(res) != reflectRes {
		t.Errorf("reflect result %v is different from normal result %v", reflectRes, res)
	}
}
