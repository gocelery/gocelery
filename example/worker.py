# Copyright (c) 2019 Sick Yoon
# This file is part of gocelery which is released under MIT license.
# See file LICENSE for full license details.

from celery import Celery

app = Celery(
    'tasks',
    broker='redis://',
    backend='redis://',
)

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_ENABLE_UTC=True,
    CELERY_TASK_PROTOCOL=1,
)


@app.task
def add(a, b):
    return a + b


@app.task
def add_reflect(a, b):
    return a + b
