package gocelery

import (
	"encoding/json"
	"fmt"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker CeleryBroker
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker) (*CeleryClient, error) {
	return &CeleryClient{broker}, nil
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := NewCeleryTask(task, args...)
	encodedMessage, err := celeryTask.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := NewCeleryMessage(encodedMessage)
	err = cc.broker.Send(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{celeryTask.ID, cc.broker}, nil
}

// AsyncResult is pending result
type AsyncResult struct {
	taskID string
	broker CeleryBroker
}

// Get gets actual result from redis
// TODO: implement retries and timeout feature
func (ar *AsyncResult) Get() (interface{}, error) {
	res, err := ar.broker.GetResult(ar.taskID)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("result not yet available")
	}

	//log.Printf("res: %s\n", string(res.([]byte)))

	var resMap map[string]interface{}
	json.Unmarshal(res.([]byte), &resMap)
	if resMap["status"] != "SUCCESS" {
		return nil, fmt.Errorf("task queue failed: %v", resMap)
	}
	return resMap["result"], nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	val, err := ar.broker.GetResult(ar.taskID)
	if err != nil {
		return false, err
	}
	return (val != nil), nil
}
