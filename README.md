# gocelery

Go Client/Server for Celery Distributed Task Queue

[![Build Status](https://travis-ci.org/shicky/gocelery.svg?branch=master)](https://travis-ci.org/shicky/gocelery)
[![Coverage Status](https://coveralls.io/repos/github/shicky/gocelery/badge.svg?branch=master)](https://coveralls.io/github/shicky/gocelery?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/shicky/gocelery)](https://goreportcard.com/report/github.com/shicky/gocelery)
[![GoDoc](https://godoc.org/github.com/shicky/gocelery?status.svg)](https://godoc.org/github.com/shicky/gocelery)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/shicky/gocelery/blob/master/LICENSE)
[![motivation](https://img.shields.io/badge/made%20with-%E2%99%A1-ff69b4.svg)](https://github.com/shicky/gocelery)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fgocelery%2Fgocelery.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fgocelery%2Fgocelery?ref=badge_shield)

## Why?

Having being involved in a number of projects migrating server from python to go, I have realized Go can help improve performance of existing python web applications.
Celery distributed tasks are used heavily in many python web applications and this library allows you to implement celery workers in Go as well as being able to submit celery tasks in Go.

You can also use this library as pure go distributed task queue.

## Go Celery Worker in Action

![demo](https://raw.githubusercontent.com/shicky/gocelery/master/demo.gif)

## Supported Brokers/Backends

Now supporting both Redis and AMQP!!

* Redis (broker/backend)
* AMQP (broker/backend) - does not allow concurrent use of channels

## Celery Configuration

Celery must be configured to use **json** instead of default **pickle** encoding.
This is because Go currently has no stable support for decoding pickle objects.
Pass below configuration parameters to use **json**.

```python
CELERY_TASK_SERIALIZER='json',
CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
CELERY_RESULT_SERIALIZER='json',
CELERY_ENABLE_UTC=True,
```

## Celery Worker Example

Run Celery Worker implemented in Go

```go
// example/worker/main.go

// Celery Task
func add(a int, b int) int {
	return a + b
}

func main() {
    // create broker and backend
	celeryBroker := gocelery.NewRedisCeleryBroker("redis://localhost:6379")
    celeryBackend := gocelery.NewRedisCeleryBackend("redis://localhost:6379")

    // use AMQP instead
    // celeryBroker := gocelery.NewAMQPCeleryBroker("amqp://")
    // celeryBackend := gocelery.NewAMQPCeleryBackend("amqp://")

	// Configure with 2 celery workers
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, celeryBackend, 2)

	// worker.add name reflects "add" task method found in "worker.py"
	celeryClient.Register("worker.add", add)

    // Start Worker - blocking method
	go celeryClient.StartWorker()

    // Wait 30 seconds and stop all workers
	time.Sleep(30 * time.Second)
	celeryClient.StopWorker()
}
```
```bash
go run example/worker/main.go
```

You can use custom struct instead to hold shared structures.

```go

type MyStruct struct {
	MyInt int
}

func (so *MyStruct) add(a int, b int) int {
	return a + b + so.MyInt
}

// code omitted ...

ms := &MyStruct{10}
celeryClient.Register("worker.add", ms.add)

// code omitted ...
```


Submit Task from Python Client
```python
# example/test.py

from celery import Celery

app = Celery('tasks',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379'
)

@app.task
def add(x, y):
    return x + y

if __name__ == '__main__':
    # submit celery task to be executed in Go workers
    ar = add.apply_async((5456, 2878), serializer='json')
    print(ar.get())
```

```bash
python example/test.py
```

## Celery Client Example

Run Celery Worker implemented in Python

```python
# example/worker.py

from celery import Celery

app = Celery('tasks',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379'
)

@app.task
def add(x, y):
    return x + y
```

```bash
cd example
celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
```

Submit Task from Go Client

```go
func main() {
    // create broker and backend
	celeryBroker := gocelery.NewRedisCeleryBroker("redis://localhost:6379")
    celeryBackend := gocelery.NewRedisCeleryBackend("redis://localhost:6379")

    // use AMQP instead
    // celeryBroker := gocelery.NewAMQPCeleryBroker("amqp://")
    // celeryBackend := gocelery.NewAMQPCeleryBackend("amqp://")

    // create client
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, celeryBackend, 0)

    // send task
	asyncResult, err := celeryClient.Delay("worker.add", 3, 5)
	if err != nil {
		panic(err)
	}

    // check if result is ready
	isReady, _ := asyncResult.Ready()
	fmt.Printf("ready status %v\n", isReady)

    // get result with 5s timeout
	res, err = asyncResult.Get(5 * time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
        fmt.Println(res)
    }
}
```

```bash
go run example/client/main.go
```

## Sample Celery Task Message

```javascript
{
    "expires": null,
    "utc": true,
    "args": [5456, 2878],
    "chord": null,
    "callbacks": null,
    "errbacks": null,
    "taskset": null,
    "id": "c8535050-68f1-4e18-9f32-f52f1aab6d9b",
    "retries": 0,
    "task": "worker.add",
    "timelimit": [null, null],
    "eta": null,
    "kwargs": {}
}
```

## Contributing

You are more than welcome to make any contributions.
Please create Pull Request for any changes.

## LICENSE

The gocelery is offered under MIT license.


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fgocelery%2Fgocelery.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fgocelery%2Fgocelery?ref=badge_large)
