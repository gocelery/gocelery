package gocelery

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"

	"github.com/go-redis/redis"
	"github.com/gocelery/gocelery/backends"
	"github.com/gocelery/gocelery/brokers"
	"github.com/gocelery/gocelery/parsers"
	"github.com/gocelery/gocelery/protocol"
)

// Worker represents distributed worker
type Worker struct {
	options         *WorkerOptions
	ctx             context.Context
	ctxCancel       context.CancelFunc
	registeredTasks sync.Map
	waitGroup       sync.WaitGroup
}

// NewWorker creates new celery workers
// number of workers is set to number of cpu cores if numWorkers smaller than zero
func NewWorker(ctx context.Context, options *WorkerOptions) (*Worker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options.sanitize()
	wCtx, cancel := context.WithCancel(ctx)
	return &Worker{
		options:   options,
		ctx:       wCtx,
		ctxCancel: cancel,
	}, nil
}

// WorkerOptions define options for worker
type WorkerOptions struct {
	Broker         Broker
	Backend        Backend
	Parser         Parser
	NumWorkers     int
	MsgProtocolVer int
}

func (o *WorkerOptions) sanitize() {
	if o.Broker == nil || o.Backend == nil {
		client := redis.NewClient(&redis.Options{})
		if o.Broker == nil {
			o.Broker = &brokers.RedisBroker{Client: client}
		}
		if o.Backend == nil {
			o.Backend = &backends.RedisBackend{Client: client}
		}
	}
	if o.Parser == nil {
		o.Parser = &parsers.JSONParser{}
	}
	if o.NumWorkers <= 0 {
		o.NumWorkers = runtime.NumCPU()
	}
	if o.MsgProtocolVer != 1 {
		o.MsgProtocolVer = 2
	}
}

// Start starts all celery workers
func (w *Worker) Start() {
	w.waitGroup.Add(w.options.NumWorkers)
	for i := 0; i < w.options.NumWorkers; i++ {
		go func(workerID int) {
			log.Printf("running worker %d", workerID)

			defer w.waitGroup.Done()

			for {
				select {
				case <-w.ctx.Done():
					return
				default:

					// get message from broker
					rawMsg, err := w.options.Broker.Get()
					if err != nil || rawMsg == nil {
						log.Printf("broker error: %v", err)
						continue
					}

					// parse task message
					// TODO: support multiple formats
					tm, err := w.options.Parser.Decode(rawMsg)
					if err != nil {
						log.Printf("parser error: %v", err)
						continue
					}

					// validate task message
					if err := tm.Validate(
						w.options.Parser.ContentType(),
						w.options.MsgProtocolVer); err != nil {
						log.Printf("task message format error: %v", err)
						continue
					}

					t, err := tm.GetTask()
					if err != nil {
						log.Printf("task translate error: %v", err)
						continue
					}

					if err := w.runTask(t); err != nil {
						log.Printf("task run error: %v", err)
					}

					// TODO: push results to backend

				}
			}
		}(i)
	}
}

// Stop gracefully stops all celery workers
// and waits for all workers to stop
func (w *Worker) Stop() {
	w.ctxCancel()
	w.waitGroup.Wait()
}

// Register new task
func (w *Worker) Register(name string, task interface{}) {
	w.registeredTasks.Store(name, task)
}

// GetTask gets registered task
func (w *Worker) GetTask(name string) (interface{}, error) {
	t, ok := w.registeredTasks.Load(name)
	if !ok {
		return nil, fmt.Errorf("failed to find registered task: %s", name)
	}
	return t, nil
}

// run namedTask
func (w *Worker) runTask(task *protocol.Task) error {

	log.Printf("running task %+v", task)

	t, err := w.GetTask(task.Name)
	if err != nil {
		return err
	}

	// use reflect to convert function pointer
	taskFunc := reflect.ValueOf(t)

	// check number of args
	if taskFunc.Type().NumIn() != len(task.Args) {
		return fmt.Errorf("number of arguments does not match")
	}

	// construct arguments
	in := make([]reflect.Value, len(task.Args))
	for i, arg := range task.Args {
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
		return nil
	}

	// TODO: construct result message

	log.Printf("result: %+v", protocol.GetRealValue(&res[0]))
	return nil
}
