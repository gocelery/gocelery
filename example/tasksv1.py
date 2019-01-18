from celery import Celery

app = Celery(
  'tasksv1',
  broker='redis://localhost',
  backend='redis://localhost'
)

app.conf.update(
  task_protocol=1,
  accept_content=['json'],
  task_serializer='json',
  event_serializer='json',
  result_serializer='json',
  timezone='America/Toronto',

  # disable heartbeat for debugging
  broker_heartbeat=0.0,
  broker_heartbeat_checkrate=0.0,
)


@app.task
def add(x, y):
  return x + y
