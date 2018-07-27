#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, logging
import aiomysql


def log(sql, args=()):
    # %s：以字符串格式输出
    # %d：以整数方式打印
    # %f：打印浮点数
    # %.2f：打印浮点数（保留两位小数）
    # %10s %8d %8.2f：‘8’‘10’指定占位符宽度
    # %-10s %-8d %-8.2f：指定占位符宽度（左对齐）
    logging.info('SQL: %s' % sql)


'''创造连接池'''


async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 全局变量，使用global声明
    # 没有用global语句的情况下，是不能修改全局变量的。
    global _pool
    # 从Python 3.5开始引入了新的语法async和await
    # 把@asyncio.coroutine替换为async；
    # 把yield from替换为await。
    _pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['db'],
        password=kw.get('charset', 'utf8'),
        autcommit=kw.get('autocommit', true),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


'''查询方法'''


async def select(sql, args, size=None):
    log(sql, args)
    global _pool
    # 获取数据库连接
    async with _pool.get() as conn:
        # 通过获取到的数据库连接conn下的cursor()方法来创建游标。
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # 通过游标cur 操作execute()方法可以写入纯sql语句。通过execute()方法中写如sql语句来对数据进行操作。
            await cur.excute(sql.replace('?', '%s'), args or ())
            # 1. cursor.fetchall()  返回[()] 或者[]
            # 2. cursor.fetchone()  () 或者None返回单个的元组，也就是一条记录(row)，如果没有结果 则返回 None
            # 3. cursor.fetchmany(size=1) [()] ,或者[] 指定size 返回多行
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await  cur.fetchall()
            logging.info('rows returned %s' % len(rs))
            return rs


'''执行方法'''


async def execute(sql, args, autocommit=true):
    log(sql)
    async with _pool.get() as conn:
        if not autocommit:
            await conn.begin()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s', args))
            affected = cur.rowcount
        if not affected:
            # conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入。
            await conn.commit()
    except BaseException as e:
        if not affected:
            await conn.rollback()
            raise
    return affected


'''用‘，’拼接字符串方法'''


def create_arg_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super.__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super.__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

'''
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('_table_', None) or name
        logging.info('found model:%s (table:%s)' % (name, tableName))
        mapings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            # isinstance()函数来判断一个对象是否是一个已知的类型，类似type()。
            # isinstance()与type()区别：
            #   type()不会认为子类是一种父类类型，不考虑继承关系。
            #   isinstance()会认为子类是一种父类类型，考虑继承关系。
            # 如果要判断两个类型是否相同推荐使用isinstance()。
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mapings[k] = v
                if v.primary_key
'''
