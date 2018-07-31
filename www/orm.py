#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# [python3 ORM重难点]:https://blog.csdn.net/haskei/article/details/57075381

import asyncio, logging

# asyncore是什么？ 他封装了HTTP UDP SSL 的异步协议 可以让单线程 也可以异步收发请求
# aiohttp，aiomysql是什么？
# 他们都是基于asyncore 实现的异步http库 异步mysql 库 调用他们就可以实现异步请求在http 和 mysql 上。
# 记住：一处异步 处处异步
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


# **kw : [python学习笔记 可变参数关键字参数**kw相关学习]:https://www.cnblogs.com/Commence/p/5578215.html
# 这里**kw 是一个dict (一个 key value 键值对)
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 全局变量，使用global声明
    # 没有用global语句的情况下，是不能修改全局变量的。
    # _pool 前面一条杠 就是非公开变量 两条杠私有变量 一条杠能调用 两条杠就不允许外部调用
    global _pool
    # 从Python 3.5开始引入了新的语法async和await
    # 把@asyncio.coroutine替换为async；
    # 把yield from替换为await。
    _pool = await aiomysql.create_pool(
        # 如果kw里面get不到'host' host就默认等于'localhost'
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
            # raise 用来抛出异常
            raise
    return affected


'''用‘，’拼接字符串方法'''

# 这个函数主要是把查询字段计数 替换成sql识别的?
# 比如说：insert into  `User` (`password`, `email`, `name`, `id`) values (?,?,?,?)  看到了么 后面这四个问号
def create_arg_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


'''
Field类的意义：
将数据库的类型与Python进行对应。
比如说我们需要对数据库建表或者增删改查 
数据库的字段不仅有不同类型 
还有是否为主键的设置，
这需要我们定一个class 类来进行定义。
'''

# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):
    '''表的字段包含名字、类型、是否为表的主键和默认值'''

    # __init__作用是初始化已实例化后的对象。
    # 子类可以不重写__init__，实例化子类时，会自动调用超类中已定义的__init__
    # 但如果重写了__init__，实例化子类时，则不会隐式的再去调用超类中已定义的__init__，会报错
    # 所以如果重写了__init__，为了能使用或扩展超类中的行为，最好显式的调用超类的__init__方法
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    '''返回 表名字 字段名 和字段类型'''

    # 在python中方法名如果是__xxxx__()的，那么就有特殊的功能，因此叫做“魔法”方法
    # 当使用print输出对象的时候，只要自己定义了__str__(self)方法，那么就会打印从在这个方法中return的数据
    # __str__你可以理解为这个类的注释 说明。
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


'''定义数据库中五个存储类型'''


class StringField(Field):

    # 这个super()方法用于多态继承的，更新变量 使用。
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


'''metaclass 元类'''


# metaclass是类的模板，所以必须从`type`类型派生：
class ModelMetaclass(type):

    # __new__控制__init__的执行，所以在其执行之前
    # cls:代表要__init__的类，此参数在实例化时由Python解释器自动提供
    # bases：代表继承父类的集合
    # attrs：类的方法集合
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
                if v.primary_key:
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise StandarError('Primary key not found.')
        for k in mapings.keys():
            # pop() 函数用于移除列表中的一个元素（默认最后一个元素），并且返回该元素的值。
            attrs.pop(k)
        # lambda 函數：
        # 冒号左边→想要传递的参数
        # 冒号右边→想要得到的数（可能带表达式）
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
            tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefualt(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self._mappings_[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # classmethod 修饰符对应的函数不需要实例化，
    # 可以直接Model.findAll()不需要实例化
    # 不需要 self 参数，
    # 但第一个参数需要是表示自身类的 cls 参数，可以来调用类的属性，类的方法，实例化对象等。
    # 可以直接调用类中的方法 cls（）.方法名（）
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        'find objects by where clause.'
        sql = [cls._select_]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('orderBy')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            # isinstance（）方法判断是否为同一类型
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.append(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s` ' % (selectField, cls._table_)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = select(' '.join(sql), args, 1)
        if len(rs) == None:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, self):
        ' find object by primary key. '
        rs = await select('%s where `%s` = ? ' % (cls._select_, cls._primary_key_), [pk], 1)
        if len(re) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefualt(), self._fields_))
        args.append(self.getValueOrDefualt(self._primary_key_))
        rows = await execute(self._insert_, args)
        if rows == 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue(), self._fields_))
        args.append(self.getValue(self._primary_key_))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self._primary_key_)]
        rows = await execute(self._delete_, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)
