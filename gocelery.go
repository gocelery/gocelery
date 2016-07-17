package gocelery

import (
	"encoding/json"
	"fmt"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
	cc.worker.Register(name, task)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker() {
	cc.worker.StartWorker()
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
	cc.worker.StopWorker()
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
	return &AsyncResult{celeryTask.ID, cc.backend}, nil
}

// AsyncResult is pending result
type AsyncResult struct {
	taskID  string
	backend CeleryBackend
}

// Get gets actual result from redis
// TODO: implement retries and timeout feature
func (ar *AsyncResult) Get() (interface{}, error) {
	res, err := ar.backend.GetResult(ar.taskID)
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
	val, err := ar.backend.GetResult(ar.taskID)
	if err != nil {
		return false, err
	}
	return (val != nil), nil
}
