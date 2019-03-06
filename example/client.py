# Copyright (c) 2019 Sick Yoon
# This file is part of gocelery which is released under MIT license.
# See file LICENSE for full license details.

from worker import add, add_reflect

ar = add_reflect.apply_async(kwargs={'a': 5456, 'b': 2878}, serializer='json')
print('Result: {}'.format(ar.get()))
