# Copyright (c) 2019 Sick Yoon
# This file is part of gocelery which is released under MIT license.
# See file LICENSE for full license details.

from celery import Celery

app = Celery(
    'tasks',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379',
)

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_ENABLE_UTC=True,
)


@app.task
def add(x, y):
    return x + y


@app.task
def add_reflect(x, y):
    return x + y
