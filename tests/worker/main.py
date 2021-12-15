import sys

from celery import Celery

app = Celery(
    imports=['tasks'],
    task_serializer='json',
    accept_content=['json'],  # Ignore other content
    result_serializer='json',
    enable_utc=True,
    ignore_result=False,
    broker_url=sys.argv[1],
    result_backend=sys.argv[2],
    task_protocol=1)


def main():
    print('starting python worker')
    app.start(argv=['worker', '-P', 'threads', '-l', 'INFO'])


if __name__ == '__main__':
    main()
