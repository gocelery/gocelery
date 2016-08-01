package gocelery

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func multiply(a int, b int) int {
	return a * b
}

func noreturn(a int, b int) {

}

func initClient() (*CeleryClient, error) {
	celeryBroker := NewRedisCeleryBroker("localhost:6379", "")
	celeryBackend := NewRedisCeleryBackend("localhost:6379", "")
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
		return
	}

	ready, err := ar.Ready()
	if err == nil {
		t.Errorf("backend is empty and should throw error since result is unavailable")
		return
	}

	go celeryClient.StartWorker()

	val, err := ar.Get(5 * time.Second)
	if err != nil {
		t.Errorf("failed to get result: %v", err)
		return
	}

	ready, err = ar.Ready()
	if err != nil {
		t.Errorf("failed to get status: %v", err)
		return
	}
	if !ready {
		t.Errorf("result should be available by now")
		return
	}

	// repeat get for cache effect
	cachedVal, err := ar.Get(1 * time.Second)
	if err != nil {
		t.Errorf("failed to get result: %v", err)
		return
	}

	if !reflect.DeepEqual(val, cachedVal) {
		t.Errorf("failed to retrieve cached val %v that should be same as %v", cachedVal, val)
		return
	}

	// number is always returned as float64
	// due to json parser limitation in golang
	actual := int(val.(float64))

	if actual != expected {
		t.Errorf("returned result %v is different from expected value %v", actual, expected)
		return
	}
	celeryClient.StopWorker()
}

func TestRegister(t *testing.T) {
	celeryClient, err := initClient()
	if err != nil {
		t.Errorf("failed to create CeleryClient: %v", err)
		return
	}
	taskName := "multiply"
	celeryClient.Register(taskName, multiply)
	task := celeryClient.worker.GetTask(taskName)
	if !reflect.DeepEqual(reflect.ValueOf(multiply), reflect.ValueOf(task)) {
		t.Errorf("registered task %v is different from received task %v", reflect.ValueOf(multiply), reflect.ValueOf(task))
		return
	}
}

func TestBlockingGet(t *testing.T) {
	celeryClient, err := initClient()
	if err != nil {
		t.Errorf("failed to create CeleryClient: %v", err)
		return
	}

	// send task
	asyncResult, err := celeryClient.Delay("dummy", 3, 5)
	if err != nil {
		t.Errorf("failed to get async result")
		return
	}

	duration := 1 * time.Second
	var asyncError error

	go func() {
		_, asyncError = asyncResult.Get(duration)
	}()

	time.Sleep(duration + time.Millisecond)
	if asyncError == nil {
		t.Errorf("failed to timeout in time")
		return
	}
}
