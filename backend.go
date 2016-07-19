package gocelery

import (
	"encoding/json"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error)
	SetResult(taskID string, result *ResultMessage) error
}

// CeleryRedisBackend is Redis implementation of CeleryBackend
type CeleryRedisBackend struct {
	*redis.Pool
}

// NewCeleryRedisBackend creates new CeleryRedisBackend
func NewCeleryRedisBackend(host, pass string) *CeleryRedisBackend {
	return &CeleryRedisBackend{
		Pool: NewRedisPool(host, pass),
	}
}

// GetResult calls API to get asynchronous result
// Should be called by AsyncResult
func (cb *CeleryRedisBackend) GetResult(taskID string) (*ResultMessage, error) {
	//"celery-task-meta-" + taskID
	conn := cb.Get()
	defer conn.Close()
	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage ResultMessage
	err = json.Unmarshal(val.([]byte), &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into backend
func (cb *CeleryRedisBackend) SetResult(taskID string, result *ResultMessage) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, resBytes)
	return err
}
