from celery import Celery

app = Celery(
    'tasks',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379'
)

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_ENABLE_UTC=True,
)

# 1) integer addition
@app.task
def add_int(x, y):
    return x + y

# 2) integer addition with named arguments
@app.task
def add_int_kwargs(x, y):
    return x + y

# 3) string addition
@app.task
def add_str(a, b):
    return a + b

# 4) integer and string concatenation
@app.task
def add_str_int(a, b):
    return a + str(b)

# 5) float addition
@app.task
def add_float(a, b):
    return a + b

# 6) maximum array length
@app.task
def max_arr_len(a, b):
    if len(a) > len(b):
        return len(a)
    return len(b)

# 7) array addition
@app.task
def add_arr(a, b):
    return a + b

# 8) dictionary addition
@app.task
def add_dict(a, b):
    c = a.copy()
    c.update(b)
    return c

# 9) boolean and operation
@app.task
def and_bool(a, b):
    return a and b

# # long running task
# @app.task(bind=True)
# def long_running_Task(self, a, b):
#     time.sleep(1)
#     self.update_state(state="PROGRESS", meta={'progress': 50})
#     time.sleep(1)
#     self.update_state(state="PROGRESS", meta={'progress': 90})
#     time.sleep(1)
#     return 'hello world: %i' % (a+b)

