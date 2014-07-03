#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

#使用SETNX和BLPOP构建的redis锁

from logging import getLogger
logger = getLogger.get(__file__)

import urandom

#使用lua是因为lua脚本的执行在redis里面是原子的
UNLOCK_LUA_SCRIPT = b"""
    if redis.call("get", KEYS[1]) == ARGV[1] then
        redis.call("lpush", KEYS[2], 1)
        return redis.call("del", KEYS[1])
    else
        return 0
    end
"""

class Lock(object):
    def __init__(self, redis_client, name, expire=None):
        self._client = redis_client
        self._name = 'lock:' + name
        self._signal = 'signal:' + name
        self._token = None


    def __enter__(self):
        pass

    acquire = __enter__

    def __exit__(self):
        pass

    release = __exit__
