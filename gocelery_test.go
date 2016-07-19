package gocelery

import (
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func multiply(a int, b int) int {
	return a * b
}

func initClient() (*CeleryClient, error) {
	celeryBroker := NewCeleryRedisBroker("localhost:6379", "")
	celeryBackend := NewCeleryRedisBackend("localhost:6379", "")
	celeryClient, err := NewCeleryClient(celeryBroker, celeryBackend, 1)
	return celeryClient, err
}

func TestWorkerClient(t *testing.T) {
	celeryClient, err := initClient()
	if err != nil {
		t.Errorf("failed to create CeleryClient: %v", err)
	}
	taskName := "multiply"
	celeryClient.Register(taskName, multiply)

	arg1 := rand.Intn(10)
	arg2 := rand.Intn(10)
	expected := arg1 * arg2
	var args []interface{}
	args = append(args, arg1)
	args = append(args, arg2)
	ar, err := celeryClient.Delay(taskName, args...)
	if err != nil {
		t.Errorf("failed to submit task: %v", err)
	}

	ready, err := ar.Ready()
	if err == nil {
		t.Errorf("backend is empty and should throw error since result is unavailable")
	}

	go celeryClient.StartWorker()

	val, err := ar.Get(5 * time.Second)
	if err != nil {
		t.Errorf("failed to get result: %v", err)
	}

	ready, err = ar.Ready()
	if err != nil {
		t.Errorf("failed to get status: %v", err)
	}
	if !ready {
		t.Errorf("result should be available by now")
	}

	//BUG - result is always returned in string?
	actual, err := strconv.Atoi(val.(string))
	if err != nil {
		t.Errorf("failed to convert result to string")
	}

	if actual != expected {
		t.Errorf("returned result %v is different from expected value %v", actual, expected)
	}
	celeryClient.StopWorker()
}

func TestRegister(t *testing.T) {
	celeryClient, err := initClient()
	if err != nil {
		t.Errorf("failed to create CeleryClient: %v", err)
	}
	taskName := "multiply"
	celeryClient.Register(taskName, multiply)
	task := celeryClient.worker.GetTask(taskName)
	if !reflect.DeepEqual(reflect.ValueOf(multiply), reflect.ValueOf(task)) {
		t.Errorf("registered task %v is different from received task %v", reflect.ValueOf(multiply), reflect.ValueOf(task))
	}
}

func TestBlockingGet(t *testing.T) {
	celeryClient, err := initClient()
	if err != nil {
		t.Errorf("failed to create CeleryClient: %v", err)
	}

	// send task
	asyncResult, err := celeryClient.Delay("dummy", 3, 5)
	if err != nil {
		t.Errorf("failed to get async result")
	}

	duration := 1 * time.Second
	var asyncError error

	go func() {
		_, asyncError = asyncResult.Get(duration)
	}()

	time.Sleep(duration + time.Millisecond)
	if asyncError == nil {
		t.Errorf("failed to timeout in time")
	}
}
