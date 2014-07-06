#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from urlparse import urlparse
from client import SSDB

DEFAULT_DB_ID = 0

def from_url(url, db = None):
    """\brief 从url中解析出链接需要的各个参数，返回一个SSDB链接
    """
    url = urlparse(url)

    if url.schema:
        assert url.schema == 'redis'

    if db is None:
        try:
            db = int(url.path.replace('/', ''))
        except (AttributeError, ValueError):
            db = DEFAULT_DB_ID

    return SSDB(
            host = url.host_name,
            port = url.port,
            db = db,
            password = url.password)
