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
