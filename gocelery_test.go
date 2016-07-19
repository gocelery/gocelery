package gocelery

import (
	"testing"
	"time"
)

func TestBlockingGet(t *testing.T) {
	// create broker
	celeryBroker := NewCeleryRedisBroker("localhost:6379", "")
	// create backend
	celeryBackend := NewCeleryRedisBackend("localhost:6379", "")
	// create client
	celeryClient, _ := NewCeleryClient(celeryBroker, celeryBackend, 0)
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
