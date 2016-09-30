package gocelery

import (
	"fmt"
	"log"
	"math/rand"
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

func getClients() ([]*CeleryClient, error) {
	redisBroker := NewRedisCeleryBroker("localhost:6379", "")
	redisBackend := NewRedisCeleryBackend("localhost:6379", "")
	redisClient, err := NewCeleryClient(redisBroker, redisBackend, 1)
	if err != nil {
		return nil, err
	}
	amqpBroker := NewAMQPCeleryBroker("amqp://")
	amqpBackend := NewAMQPCeleryBackend("amqp://")
	amqpClient, err := NewCeleryClient(amqpBroker, amqpBackend, 1)
	if err != nil {
		return nil, err
	}
	_ = amqpClient
	return []*CeleryClient{
		redisClient,
		amqpClient,
	}, nil
}

func TestWorkerClientKwargs(t *testing.T) {
	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create CeleryClients: %v", err)
	}
	taskName := "multiply_kwargs"
	for _, celeryClient := range celeryClients {

		celeryClient.Register(taskName, &multiplyKwargs{})
		go celeryClient.StartWorker()

		arg1 := rand.Intn(10)
		arg2 := rand.Intn(10)
		expected := arg1 * arg2
		ar, err := celeryClient.DelayKwargs(taskName, map[string]interface{}{
			"a": arg1,
			"b": arg2,
		})
		if err != nil {
			t.Errorf("failed to submit task: %v", err)
			return
		}
		val, err := ar.Get(5 * time.Second)
		if err != nil {
			t.Errorf("failed to get result: %v", err)
			return
		}
		actual := int(val.(float64))
		if actual != expected {
			t.Errorf("returned result %v is different from expected value %v", actual, expected)
			return
		}

		celeryClient.StopWorker()
	}

}

func TestWorkerClientArgs(t *testing.T) {
	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create CeleryClients: %v", err)
	}
	taskName := "multiply"

	for _, celeryClient := range celeryClients {

		celeryClient.Register("multiply", multiply)
		go celeryClient.StartWorker()

		log.Printf("worker started")

		arg1 := rand.Intn(10)
		arg2 := rand.Intn(10)
		expected := arg1 * arg2

		ar, err := celeryClient.Delay(taskName, arg1, arg2)
		if err != nil {
			t.Errorf("failed to submit task: %v", err)
			return
		}

		log.Printf("async result got")

		val, err := ar.Get(5 * time.Second)
		if err != nil {
			t.Errorf("failed to get result: %v", err)
			return
		}
		actual := int(val.(float64))
		if actual != expected {
			t.Errorf("returned result %v is different from expected value %v", actual, expected)
			return
		}
		celeryClient.StopWorker()
	}

}

/*
func TestWorkerClientArgs(t *testing.T) {

	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create CeleryClient: %v", err)
	}
	taskName := "multiply"

	for _, celeryClient := range celeryClients {
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
}
*/

/*
func TestRegister(t *testing.T) {
	celeryClients, err := getClients()
	if err != nil {
		t.Errorf("failed to create CeleryClients: %v", err)
		return
	}
	taskName := "multiply"

	for _, celeryClient := range celeryClients {
		celeryClient.Register(taskName, multiply)
		task := celeryClient.worker.GetTask(taskName)
		if !reflect.DeepEqual(reflect.ValueOf(multiply), reflect.ValueOf(task)) {
			t.Errorf("registered task %v is different from received task %v", reflect.ValueOf(multiply), reflect.ValueOf(task))
			return
		}
	}
}

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
