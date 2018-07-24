#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, logging
import  aiomysql

def log(sql, args= ()):
    logging.info('SQL: %s' %sql)

async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global _pool
    _pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['db'],
        password = kw.get('charset', 'utf8'),
        autcommit = kw.get('autocommit', true),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )

async def select(sql,args,size = None):
    log(sql,args)
    global _pool
    async with _pool.get() as conn :
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.excute(sql.replace('?','%s'),args or())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await  cur.fetchall()
            logging.info('rows returned %s' %len(rs))
            return rs

