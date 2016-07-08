# gocelery

Go Client for Celery Distributed Task Queue

[![Build Status](https://travis-ci.org/shicky/gocelery.svg?branch=master)](https://travis-ci.org/shicky/gocelery)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/shicky/go-gorilla-skeleton/blob/master/LICENSE)
[![motivation](https://img.shields.io/badge/made%20with-%E2%99%A1-ff69b4.svg)](https://github.com/shicky/go-gorilla-skeleton)

## Why?

Having being involved in a number of projects migrating server from python to go, I have realized many of celery distributed tasks cannot be easily converted.
Simply because Python still has abundance of useful third-party libraries available.

## Supported Brokers

We are currently only supporting Redis but will add support for RabbitMQ soon.

* Redis

## Dependencies

* go get gopkg.in/redis.v4
* go get github.com/satori/go.uuid

## Example

```go
func main() {
    celeryBroker, _ := NewCeleryRedisBroker("localhost:6379", "", 0)
    celeryClient, _ := NewCeleryClient(celeryBroker)
    celeryClient.SendMessage(NewCeleryTask())
}
```

## Sample Celery Message

```javascript
{
    "id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77",
    "task": "celery.task.PingTask",
    "args": [],
    "kwargs": {},
    "retries": 0,
    "eta": "2009-11-17T12:30:56.527191"
}
```

## LICENSE

The gocelery is offered under MIT license.
