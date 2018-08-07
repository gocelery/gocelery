package gocelery

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func multiply(a int, b int) int {
	return a * b
}

type multiplyKwargs struct {
	a int
	b int
}

func (m *multiplyKwargs) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	m.a = int(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	m.b = int(kwargBFloat)
	return nil
}

func (m *multiplyKwargs) RunTask() (interface{}, error) {
	result := m.a * m.b
	return result, nil
}

func getAMQPClient() (*CeleryClient, error) {
	amqpBroker := NewAMQPCeleryBroker("amqp://")
	amqpBackend := NewAMQPCeleryBackend("amqp://")
	return NewCeleryClient(amqpBroker, amqpBackend, 4)
}

func getRedisClient() (*CeleryClient, error) {
	redisBroker := NewRedisCeleryBroker("redis://localhost:6379")
	redisBackend := NewRedisCeleryBackend("redis://localhost:6379")
	return NewCeleryClient(redisBroker, redisBackend, 1)
}

func getClients() ([]*CeleryClient, error) {
	redisClient, err := getRedisClient()
	if err != nil {
		return nil, err
	}
	amqpClient, err := getAMQPClient()
	if err != nil {
		return nil, err
	}
	return []*CeleryClient{
		redisClient,
		amqpClient,
	}, nil
}

func debugLog(client *CeleryClient, format string, args ...interface{}) {
	pre := fmt.Sprintf("client[%p] - ", client)
	log.Printf(pre+format, args...)
}

func TestWorkerClient(t *testing.T) {

	// prepare clients
	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create clients")
		return
	}

	for i := 0; i < 2; i++ {

		kwargTaskName := generateUUID()
		kwargTask := &multiplyKwargs{}

		argTaskName := generateUUID()
		argTask := multiply

		for j := 0; j < 2; j++ {
			for _, celeryClient := range celeryClients {

				debugLog(celeryClient, "registering kwarg task %s %p", kwargTaskName, kwargTask)
				celeryClient.Register(kwargTaskName, kwargTask)

				debugLog(celeryClient, "registering arg task %s %p", argTaskName, argTask)
				celeryClient.Register(argTaskName, argTask)

				debugLog(celeryClient, "starting worker")
				celeryClient.StartWorker()

				arg1 := rand.Intn(100)
				arg2 := rand.Intn(100)
				expected := arg1 * arg2

				debugLog(celeryClient, "submitting tasks")
				kwargAsyncResult, err := celeryClient.DelayKwargs(kwargTaskName, map[string]interface{}{
					"a": arg1,
					"b": arg2,
				})
				if err != nil {
					t.Errorf("failed to submit kwarg task %s: %v", kwargTaskName, err)
					return
				}

				argAsyncResult, err := celeryClient.Delay(argTaskName, arg1, arg2)
				if err != nil {
					t.Errorf("failed to submit arg task %s: %v", argTaskName, err)
					return
				}

				debugLog(celeryClient, "waiting for result")
				kwargVal, err := kwargAsyncResult.Get(10 * time.Second)
				if err != nil {
					t.Errorf("failed to get result: %v", err)
					return
				}

				debugLog(celeryClient, "validating result")
				actual := int(kwargVal.(float64))
				if actual != expected {
					t.Errorf("returned result %v is different from expected value %v", actual, expected)
					return
				}

				debugLog(celeryClient, "waiting for result")
				argVal, err := argAsyncResult.Get(10 * time.Second)
				if err != nil {
					t.Errorf("failed to get result: %v", err)
					return
				}

				debugLog(celeryClient, "validating result")
				actual = int(argVal.(float64))
				if actual != expected {
					t.Errorf("returned result %v is different from expected value %v", actual, expected)
					return
				}

				debugLog(celeryClient, "stopping worker")
				celeryClient.StopWorker()
			}
		}
	}

}

func TestRegister(t *testing.T) {
	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create CeleryClients: %v", err)
		return
	}
	taskName := generateUUID()

	for _, celeryClient := range celeryClients {
		celeryClient.Register(taskName, multiply)
		task := celeryClient.worker.GetTask(taskName)
		if !reflect.DeepEqual(reflect.ValueOf(multiply), reflect.ValueOf(task)) {
			t.Errorf("registered task %v is different from received task %v", reflect.ValueOf(multiply), reflect.ValueOf(task))
			return
		}
	}
}

/*
func TestBlockingGet(t *testing.T) {
	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create CeleryClients: %v", err)
		return
	}

	for _, celeryClient := range celeryClients {

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
}
*/
