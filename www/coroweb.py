#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# os, inspect 是什么包
import asyncio, os, inspect, logging, functools

from urllib import parse
from aiohttp import web
from apis import APIError

def get(path):
    ' @get装饰器，给处理函数绑定URL和HTTP method-GET的属性 '
    def decorator(func):
        # 这个注解什么意思
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper._method_ = 'POST'
        wrapper._route_ = path
        return wrapper
    return decorator



