from celery import Celery

'''
app = Celery('tasks',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379'
)
'''
app = Celery('tasks', backend='amqp', broker='amqp://')

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_ENABLE_UTC=True,
)

@app.task
def add(x, y):
    return x + y
