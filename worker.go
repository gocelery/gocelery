package gocelery

import (
	"fmt"
	"log"
	"reflect"
	"sync"
)

// CeleryWorker represents distributed task worker
type CeleryWorker struct {
	broker          CeleryBroker
	numWorkers      int
	registeredTasks map[string]interface{}
	stopChannel     chan bool
	workWG          sync.WaitGroup
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, numWorkers int) *CeleryWorker {
	return &CeleryWorker{
		broker:          broker,
		numWorkers:      numWorkers,
		registeredTasks: make(map[string]interface{}),
	}
}

// StartWorker starts celery worker
func (w *CeleryWorker) StartWorker() {
	w.stopChannel = make(chan bool, 1)
	w.workWG.Add(w.numWorkers)
	for i := 0; i < w.numWorkers; i++ {
		go func() {
			defer w.workWG.Done()
			for {
				select {
				case <-w.stopChannel:
					return
				default:
					// process messages
					taskMessage := w.broker.GetTaskMessage()
					if taskMessage == nil {
						continue
					}

					log.Printf("task message received: %v\n", taskMessage)

					err := w.RunTask(taskMessage)
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
func (w *CeleryWorker) RunTask(message *TaskMessage) error {
	task := w.GetTask(message.Task)
	if task == nil {
		return fmt.Errorf("task %s is not registered", message.Task)
	}
	taskFunc := reflect.ValueOf(task)

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return fmt.Errorf("Number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}
	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i)
		msgType := reflect.TypeOf(arg)
		// special case - convert float64 to int
		if origType == reflect.TypeOf(0) && msgType == reflect.TypeOf(0.0) {
			// convert arg from float64 to int
			//log.Println("converting to float")
			in[i] = reflect.ValueOf(int(arg.(float64)))
		} else {
			in[i] = reflect.ValueOf(arg)
		}
	}

	// call method
	res := taskFunc.Call(in)
	log.Println(res[0])

	// push result to broker
	return nil
}
