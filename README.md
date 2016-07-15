# gocelery

Go Client/Server for Celery Distributed Task Queue

[![Build Status](https://travis-ci.org/shicky/gocelery.svg?branch=master)](https://travis-ci.org/shicky/gocelery)
[![Go Report Card](https://goreportcard.com/badge/github.com/shicky/gocelery)](https://goreportcard.com/report/github.com/shicky/gocelery)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/shicky/go-gorilla-skeleton/blob/master/LICENSE)
[![motivation](https://img.shields.io/badge/made%20with-%E2%99%A1-ff69b4.svg)](https://github.com/shicky/go-gorilla-skeleton)

## Why?

Having being involved in a number of projects migrating server from python to go, I have realized Go can help improve performance of existing python web applications.
Celery distributed tasks are used heavily in many python web applications and this library allows you to implement celery workers in Go as well as being able to submit celery tasks in Go.

## Supported Brokers/Backend

We are currently only supporting Redis but will add support for RabbitMQ soon.
Currently broker and backend database must be same.

* Redis

## Dependencies

* go get github.com/garyburd/redigo/redis
* go get github.com/satori/go.uuid

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
// Celery Task
func add(a int, b int) int {
	return a + b
}

func main() {
	celeryBroker := gocelery.NewCeleryRedisBroker("localhost:6379", "")
    // Configure with 2 celery workers
	celeryClient, _ := gocelery.NewCeleryClient(celeryBroker, 2)
    // worker.add name reflects "add" task method found in "worker.py"
	celeryClient.Register("worker.add", add)
    // Start Worker - blocking method
	go celeryClient.StartWorker()
    // Wait 30 seconds and stop all workers
	time.Sleep(30 * time.Second)
	celeryClient.StopWorker()
}
```

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
    # submit celery task to be executed in Go workers
    ar = add.apply_async((5456, 2878), serializer='json')
    print(ar.get())
```

## Celery Client Example

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

Submit Task from Go Client

```go
func main() {
    // create broker
    celeryBroker, _ := gocelery.NewCeleryRedisBroker("localhost:6379", "", 0)
    // create client
    celeryClient, _ := gocelery.NewCeleryClient(celeryBroker)
    // send task
    asyncResult, _ := celeryClient.Delay("worker.add", 3, 2)

    // wait until result is ready
    isReady := asyncResult.Ready()
    for isReady == false {
        isReady = asyncResult.Ready()
        time.Sleep(1 * time.Second)
    }

    // get the result
    res := asyncResult.Get()
    fmt.Println(res)
}
```

## Test

Take a look at example code under example directory.

Run celery worker in python
```bash
cd example
celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
```

Submit celery task in python
```bash
python example/test.py
```

Run celery worker in Go
```bash
go run example/worker/main.go
```

Submit celery task in Go
```bash
go run example/client/main.go
```

Monitor Redis Message
```bash
redis-cli monitor
```

<!--
## Sample Celery Message

Redis Message

```javascript
"LPUSH" "celery" "{
    \"body\": """,
    \"headers\": {},
    \"content-type\": \"application/json\",
    \"properties\": {
        \"body_encoding\": \"base64\",
        \"correlation_id\": \"3aa6ea4b-b761-4283-868b-e6f5a708a74f\",
        \"reply_to\": \"283a98ec-8687-3125-9074-4374be1a09fa\",
        \"delivery_info\": {
            \"priority\": 0,
            \"routing_key\": \"celery\",
            \"exchange\": \"celery\"
        },
        \"delivery_mode\": 2,
        \"delivery_tag\": \"decb2023-6301-4d98-aa35-b30f605cd0e4\"
    },
    \"content-encoding\": \"utf-8\"
}"
```

Get Answer
```javascript
"GET" "celery-task-meta-c8535050-68f1-4e18-9f32-f52f1aab6d9b"
```

Decoded Body

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
-->

## Contributing

You are more than welcome to make any contributions.
Please create Pull Request for any changes.

I need help on following items:
* Separating broker/backend
* Supporting other brokers/backends such as RabbitMQ
* Implementing more comprehensive tests

## LICENSE

The gocelery is offered under MIT license.
