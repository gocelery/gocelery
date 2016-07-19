from worker import add

ar = add.apply_async((5456, 2878), serializer='json')
print('Ready status: {}'.format(ar.ready()))
print('Result: {}'.format(ar.get()))
