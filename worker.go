package gocelery

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/satori/go.uuid"
)

// CeleryWorker represents distributed task worker
type CeleryWorker struct {
	broker          CeleryBroker
	backend         CeleryBackend
	numWorkers      int
	registeredTasks map[string]interface{}
	stopChannel     chan bool
	workWG          sync.WaitGroup
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, backend CeleryBackend, numWorkers int) *CeleryWorker {
	return &CeleryWorker{
		broker:          broker,
		backend:         backend,
		numWorkers:      numWorkers,
		registeredTasks: make(map[string]interface{}),
	}
}

// StartWorker starts celery worker
func (w *CeleryWorker) StartWorker() {
	w.stopChannel = make(chan bool, 1)
	w.workWG.Add(w.numWorkers)
	for i := 0; i < w.numWorkers; i++ {
		// generate uuid
		workerID := uuid.NewV4().String()
		go func() {
			defer w.workWG.Done()
			for {
				select {
				case <-w.stopChannel:
					return
				default:
					// process messages
					taskMessage, err := w.broker.GetTaskMessage()
					if err != nil || taskMessage == nil {
						continue
					}

					log.Printf("WORKER %s task message received: %v\n", workerID, taskMessage)

					// run task
					val, err := w.RunTask(taskMessage)
					if err != nil {
						log.Println(err)
						continue
					}

					// push result to backend
					err = w.backend.SetResult(taskMessage.ID, NewResultMessage(val))
					if err != nil {
						log.Println(err)
						continue
					}

				}
			}
		}()
	}
	// wait until all tasks are done
	w.workWG.Wait()
}

// StopWorker stops celery workers
func (w *CeleryWorker) StopWorker() {
	// stops celery workers
	w.stopChannel <- true
}

// GetNumWorkers returns number of currently running workers
func (w *CeleryWorker) GetNumWorkers() int {
	return w.numWorkers
}

// Register registers tasks (functions)
func (w *CeleryWorker) Register(name string, task interface{}) {
	w.registeredTasks[name] = task
}

// GetTask retrieves registered task
func (w *CeleryWorker) GetTask(name string) interface{} {
	task, ok := w.registeredTasks[name]
	if !ok {
		return nil
	}
	return task
}

// RunTask runs celery task
func (w *CeleryWorker) RunTask(message *TaskMessage) (*reflect.Value, error) {
	task := w.GetTask(message.Task)
	if task == nil {
		return nil, fmt.Errorf("task %s is not registered", message.Task)
	}
	taskFunc := reflect.ValueOf(task)

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("Number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}
	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}
	return &res[0], nil
}
