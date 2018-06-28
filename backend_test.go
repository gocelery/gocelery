package gocelery

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func getBackends() []CeleryBackend {
	return []CeleryBackend{
		NewRedisCeleryBackend("redis://localhost:6379"),
		NewAMQPCeleryBackend("amqp://"),
	}
}

// TestGetResult is Redis specific test to get result from backend
func TestGetResult(t *testing.T) {
	backend := NewRedisCeleryBackend("redis://localhost:6379")
	taskID := generateUUID()

	// value must be float64 for testing due to json limitation
	value := reflect.ValueOf(rand.Float64())
	resultMessage := getReflectionResultMessage(&value)
	defer releaseResultMessage(resultMessage)
	messageBytes, err := json.Marshal(resultMessage)
	if err != nil {
		t.Errorf("error marshalling result message: %v", err)
	}
	conn := backend.Get()
	defer conn.Close()
	_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, messageBytes)
	if err != nil {
		t.Errorf("error setting result message to celery: %v", err)
	}
	// get result
	res, err := backend.GetResult(taskID)
	if err != nil {
		t.Errorf("error getting result from backend: %v", err)
	}
	if !reflect.DeepEqual(res, resultMessage) {
		t.Errorf("result message received %v is different from original %v", res, resultMessage)
	}
}

// TestSetResult is Redis specific test to set result to backend
func TestSetResult(t *testing.T) {
	backend := NewRedisCeleryBackend("redis://localhost:6379")
	taskID := generateUUID()
	value := reflect.ValueOf(rand.Float64())
	resultMessage := getReflectionResultMessage(&value)
	releaseResultMessage(resultMessage)
	// set result
	err := backend.SetResult(taskID, resultMessage)
	if err != nil {
		t.Errorf("error setting result to backend: %v", err)
	}
	conn := backend.Get()
	defer conn.Close()
	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
	if err != nil {
		t.Errorf("error getting data from redis: %v", err)
	}
	if val == nil {
		t.Errorf("result not available from redis")
	}
	var res ResultMessage
	err = json.Unmarshal(val.([]byte), &res)
	if err != nil {
		t.Errorf("error parsing json result")
	}
	if !reflect.DeepEqual(&res, resultMessage) {
		t.Errorf("result message received %v is different from original %v", &res, resultMessage)
	}
}

// TestSetGetResult tests set/get result feature for all backends
func TestSetGetResult(t *testing.T) {
	for _, backend := range getBackends() {
		taskID := generateUUID()
		value := reflect.ValueOf(rand.Float64())
		resultMessage := getReflectionResultMessage(&value)
		defer releaseResultMessage(resultMessage)
		// set result
		err := backend.SetResult(taskID, resultMessage)
		if err != nil {
			t.Errorf("error setting result to backend: %v", err)
		}
		// get result
		res, err := backend.GetResult(taskID)
		if err != nil {
			t.Errorf("error getting result from backend: %v", err)
		}
		if !reflect.DeepEqual(res, resultMessage) {
			t.Errorf("result message received %v is different from original %v", res, resultMessage)
		}
	}
}
