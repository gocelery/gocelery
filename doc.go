// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

/*
Package gocelery is Celery Distributed Task Queue in Go

Celery distributed tasks are used heavily in many python web applications and this library allows you to implement celery workers in Go as well as being able to submit celery tasks in Go.

This package can also be used as pure go distributed task queue.

Supported brokers/backends

    * Redis (broker/backend)
    * AMQP (broker/backend)

Celery must be configured to use json instead of default pickle encoding. This is because Go currently has no stable support for decoding pickle objects. Pass below configuration parameters to use json.

    CELERY_TASK_SERIALIZER='json'
    CELERY_ACCEPT_CONTENT=['json']  # Ignore other content
    CELERY_RESULT_SERIALIZER='json'
    CELERY_ENABLE_UTC=True
*/
package gocelery
