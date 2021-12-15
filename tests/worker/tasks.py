from main import app


@app.task
def subtract(a, b):
    print('subtracting...')
    return a - b
