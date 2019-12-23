# gocelery

Go Client/Server for Celery Distributed Task Queue

[![Build Status](https://github.com/gocelery/gocelery/workflows/Go/badge.svg)](https://github.com/gocelery/gocelery/workflows/Go/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/gocelery/gocelery/badge.svg?branch=master)](https://coveralls.io/github/gocelery/gocelery?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/gocelery/gocelery)](https://goreportcard.com/report/github.com/gocelery/gocelery)
[!["Open Issues"](https://img.shields.io/github/issues-raw/gocelery/gocelery.svg)](https://github.com/gocelery/gocelery/issues)
[!["Latest Release"](https://img.shields.io/github/release/gocelery/gocelery.svg)](https://github.com/gocelery/gocelery/releases/latest)
[![GoDoc](https://godoc.org/github.com/gocelery/gocelery?status.svg)](https://godoc.org/github.com/gocelery/gocelery)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/gocelery/gocelery/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fgocelery%2Fgocelery.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fgocelery%2Fgocelery?ref=badge_shield)

## Why?

Having been involved in several projects migrating servers from Python to Go, I have realized Go can improve performance of existing python web applications.
As Celery distributed tasks are often used in such web applications, this library allows you to both implement celery workers and submit celery tasks in Go.

You can also use this library as pure go distributed task queue.

## Go Celery Worker in Action

![demo](https://raw.githubusercontent.com/gocelery/gocelery/master/demo.gif)

## Supported Brokers/Backends

Now supporting both Redis and AMQP!!

* Redis (broker/backend)
* AMQP (broker/backend) - does not allow concurrent use of channels

## Celery Configuration

Celery must be configured to use **json** instead of default **pickle** encoding.
This is because Go currently has no stable support for decoding pickle objects.
Pass below configuration parameters to use **json**.

Starting from version 4.0, Celery uses message protocol version 2 as default value.
GoCelery does not yet support message protocol version 2, so you must explicitly set `CELERY_TASK_PROTOCOL` to 1.

```python
CELERY_TASK_SERIALIZER='json',
CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
CELERY_RESULT_SERIALIZER='json',
CELERY_ENABLE_UTC=True,
CELERY_TASK_PROTOCOL=1,
```

## Example

[GoCelery GoDoc](https://godoc.org/github.com/gocelery/gocelery) has good examples.<br/>
Also take a look at `example` directory for sample python code.

### GoCelery Worker Example

Run Celery Worker implemented in Go

```go
// create redis connection pool
redisPool := &redis.Pool{
  Dial: func() (redis.Conn, error) {
		c, err := redis.DialURL("redis://")
		if err != nil {
			return nil, err
		}
		return c, err
	},
}

// initialize celery client
cli, _ := gocelery.NewCeleryClient(
	gocelery.NewRedisBroker(redisPool),
	&gocelery.RedisCeleryBackend{Pool: redisPool},
	5, // number of workers
)

// task
add := func(a, b int) int {
	return a + b
}

// register task
cli.Register("worker.add", add)

// start workers (non-blocking call)
cli.StartWorker()

// wait for client request
time.Sleep(10 * time.Second)

// stop workers gracefully (blocking call)
cli.StopWorker()
```

### Python Client Example

Submit Task from Python Client

```python
from celery import Celery

app = Celery('tasks',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379'
)

@app.task
def add(x, y):
    return x + y

if __name__ == '__main__':
    ar = add.apply_async((5456, 2878), serializer='json')
    print(ar.get())
```

### Python Worker Example

Run Celery Worker implemented in Python

```python
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
celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
```

### GoCelery Client Example

Submit Task from Go Client

```go
// create redis connection pool
redisPool := &redis.Pool{
  Dial: func() (redis.Conn, error) {
		c, err := redis.DialURL("redis://")
		if err != nil {
			return nil, err
		}
		return c, err
	},
}

// initialize celery client
cli, _ := gocelery.NewCeleryClient(
	gocelery.NewRedisBroker(redisPool),
	&gocelery.RedisCeleryBackend{Pool: redisPool},
	1,
)

// prepare arguments
taskName := "worker.add"
argA := rand.Intn(10)
argB := rand.Intn(10)

// run task
asyncResult, err := cli.Delay(taskName, argA, argB)
if err != nil {
	panic(err)
}

// get results from backend with timeout
res, err := asyncResult.Get(10 * time.Second)
if err != nil {
	panic(err)
}

log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))
```

## Sample Celery Task Message

Celery Message Protocol Version 1

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

## Projects

Please let us know if you use gocelery in your project!

## Contributing

You are more than welcome to make any contributions.
Please create Pull Request for any changes.

## LICENSE

The gocelery is offered under MIT license.
