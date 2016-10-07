from worker import add, add_reflect

#ar = add.apply_async((5456, 2878), serializer='json')
#print('Ready status: {}'.format(ar.ready()))
#print('Result: {}'.format(ar.get()))

ar = add_reflect.apply_async(kwargs={'x': 5456, 'y': 2878}, serializer='json')
print('Result: {}'.format(ar.get()))

