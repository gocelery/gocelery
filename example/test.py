from worker import add

ar = add.apply_async((5456, 2878), serializer='json')
ar.get()
