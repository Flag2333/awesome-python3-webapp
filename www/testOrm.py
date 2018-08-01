#!usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import orm
from model import User, Blog, Comment

async def test(loop):
    await orm.create_pool(loop=loop, host='localhost', port=3306, user='root', password='', db='webapp')
    u = User(name='Test10', email='test10@example.com', passwd='123456', image='about:blank')
    await u.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()