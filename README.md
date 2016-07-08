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

## Test

Start worker from example directory.

```bash
celery -A worker worker --loglevel=info
```

Run test.py to test if celery worker is listening.

```bash
python -m
```

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



## LICENSE

The gocelery is offered under MIT license.
