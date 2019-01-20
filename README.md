# gocelery.v2 (UNDER DEVELOPMENT)

Go Client/Server for Celery Distributed Task Queue

[![Build Status](https://travis-ci.org/gocelery/gocelery.svg?branch=v2)](https://travis-ci.org/gocelery/gocelery)
[![Coverage Status](https://coveralls.io/repos/github/gocelery/gocelery/badge.svg?branch=v2)](https://coveralls.io/github/gocelery/gocelery?branch=v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/gocelery/gocelery)](https://goreportcard.com/report/github.com/gocelery/gocelery)
[![GoDoc](https://godoc.org/github.com/gocelery/gocelery?status.svg)](https://godoc.org/github.com/gocelery/gocelery)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/gocelery/gocelery/blob/master/LICENSE)
[![motivation](https://img.shields.io/badge/made%20with-%E2%99%A1-ff69b4.svg)](https://github.com/gocelery/gocelery)

## Usage

```
go get gopkg.in/gocelery/gocelery.v2
```

## v2 Design

* Message Protocol v2 support
* structs should be initialized by caller instead of using constructor
* use context.Context

## Try it out!

```
go run example/v2/args/worker.go
```

```
python example/v2/client_args.py
```

## Why?

Having being involved in a number of projects migrating server from python to go, I have realized Go can help improve performance of existing python web applications.
Celery distributed tasks are used heavily in many python web applications and this library allows you to implement celery workers in Go as well as being able to submit celery tasks in Go.

You can also use this library as pure go distributed task queue.

## Contributing

You are more than welcome to make any contributions.
Please create Pull Request for any changes.

## LICENSE

The gocelery is offered under MIT license.
