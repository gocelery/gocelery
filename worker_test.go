// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

// add is test task method
func add(a int, b int) int {
	return a + b
}

// TestWorkerRegisterTask tests whether a task can be registered and retrieved correctly
func TestWorkerRegisterTask(t *testing.T) {
	testCases := []struct {
		name           string
		broker         CeleryBroker
		backend        CeleryBackend
		registeredTask interface{}
	}{
		{
			name:           "register task with redis broker/backend",
			broker:         redisBroker,
			backend:        redisBackend,
			registeredTask: add,
		},
		{
			name:           "register task with amqp broker/backend",
			broker:         amqpBroker,
			backend:        amqpBackend,
			registeredTask: add,
		},
	}
	for _, tc := range testCases {
		celeryWorker := NewCeleryWorker(tc.broker, tc.backend, 1)
		taskName := uuid.Must(uuid.NewV4()).String()
		celeryWorker.Register(taskName, tc.registeredTask)
		receivedTask := celeryWorker.GetTask(taskName)
		if !reflect.DeepEqual(
			reflect.ValueOf(receivedTask),
			reflect.ValueOf(tc.registeredTask),
		) {
			t.Errorf("test '%s': expected registered task %+v but received %+v", tc.name, tc.registeredTask, receivedTask)
		}
	}
}

// TestWorkerRunTask tests successful function execution
func TestWorkerRunTask(t *testing.T) {
	testCases := []struct {
		name           string
		broker         CeleryBroker
		backend        CeleryBackend
		registeredTask interface{}
	}{
		{
			name:           "run task with redis broker/backend",
			broker:         redisBroker,
			backend:        redisBackend,
			registeredTask: add,
		},
		{
			name:           "run task with amqp broker/backend",
			broker:         amqpBroker,
			backend:        amqpBackend,
			registeredTask: add,
		},
	}
	for _, tc := range testCases {
		celeryWorker := NewCeleryWorker(tc.broker, tc.backend, 1)
		taskName := uuid.Must(uuid.NewV4()).String()
		celeryWorker.Register(taskName, tc.registeredTask)
		args := []interface{}{
			rand.Int(),
			rand.Int(),
		}
		res := add(args[0].(int), args[1].(int))
		taskMessage := &TaskMessage{
			ID:      uuid.Must(uuid.NewV4()).String(),
			Task:    taskName,
			Args:    args,
			Kwargs:  nil,
			Retries: 1,
			ETA:     nil,
		}
		resultMsg, err := celeryWorker.RunTask(taskMessage)
		if err != nil {
			t.Errorf("test '%s': failed to run celery task %v: %v", tc.name, taskMessage, err)
			continue
		}
		reflectRes := resultMsg.Result.(int64)
		if int64(res) != reflectRes {
			t.Errorf("test '%s': reflect result %v is different from normal result %v", tc.name, reflectRes, res)
		}
	}
}

// TestWorkerExpiredTask tests expired tasks
func TestWorkerExpiredTask(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name           string
		broker         CeleryBroker
		backend        CeleryBackend
		expires        *time.Time
		registeredTask interface{}
	}{
		{
			name:           "run expired task with redis broker/backend",
			broker:         redisBroker,
			backend:        redisBackend,
			expires:        &now,
			registeredTask: add,
		},
		{
			name:           "run expired task with amqp broker/backend",
			broker:         amqpBroker,
			backend:        amqpBackend,
			expires:        &now,
			registeredTask: add,
		},
	}
	for _, tc := range testCases {
		celeryWorker := NewCeleryWorker(tc.broker, tc.backend, 1)
		taskName := uuid.Must(uuid.NewV4()).String()
		celeryWorker.Register(taskName, tc.registeredTask)
		args := []interface{}{
			rand.Int(),
			rand.Int(),
		}
		taskMessage := &TaskMessage{
			ID:      uuid.Must(uuid.NewV4()).String(),
			Task:    taskName,
			Args:    args,
			Kwargs:  nil,
			Retries: 1,
			ETA:     nil,
			Expires: tc.expires,
		}
		if _, err := celeryWorker.RunTask(taskMessage); err == nil {
			t.Errorf("test '%s': expired task was not ignored", tc.name)
		}
	}
}

// TestWorkerNumWorkers ensures correct number of workers is set
func TestWorkerNumWorkers(t *testing.T) {
	testCases := []struct {
		name    string
		broker  CeleryBroker
		backend CeleryBackend
	}{
		{
			name:    "ensure correct number of workers with redis broker/backend",
			broker:  redisBroker,
			backend: redisBackend,
		},
		{
			name:    "ensure correct number of workers with amqp broker/backend",
			broker:  amqpBroker,
			backend: amqpBackend,
		},
	}
	for _, tc := range testCases {
		numWorkers := rand.Intn(10)
		celeryWorker := NewCeleryWorker(tc.broker, tc.backend, numWorkers)
		celeryNumWorkers := celeryWorker.GetNumWorkers()
		if numWorkers != celeryNumWorkers {
			t.Errorf("test '%s': number of workers are different: %d vs %d", tc.name, numWorkers, celeryNumWorkers)
		}
	}
}

// TestWorkerStartStop tests starting and stopping workers
// and gracefully wait for all workers to terminate
// ensure test timeout is set to avoid hanging
func TestWorkerStartStop(t *testing.T) {
	testCases := []struct {
		name    string
		broker  CeleryBroker
		backend CeleryBackend
	}{
		{
			name:    "start and gracefully stop workers with redis broker/backend",
			broker:  redisBroker,
			backend: redisBackend,
		},
		{
			name:    "start and gracefully stop workers with amqp broker/backend",
			broker:  amqpBroker,
			backend: amqpBackend,
		},
	}
	for _, tc := range testCases {
		celeryWorker := NewCeleryWorker(tc.broker, tc.backend, 1000)
		go celeryWorker.StartWorker()
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		go func() {
			celeryWorker.StopWorker()
			cancel()
		}()
		func() {
			for {
				select {
				case <-ctx.Done():
					if ctx.Err() != context.Canceled {
						t.Errorf("test '%s': failed to stop celery workers in time: %+v", tc.name, ctx.Err())
					}
					return
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}
}
