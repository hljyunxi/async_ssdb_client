#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from urlparse import urlparse

import connection

class StrictSSDB(object):
    pass


class SSDB(StrictSSDB):
    pass


class SSDBBatch(object):
    pass


def from_url(url):
    """\brief 从url中解析出链接需要的各个参数，返回一个SSDB链接
    """
    url = urlparse(url)

    if url.schema:
        assert url.schema == 'ssdb'

    return SSDB(
            host = url.host_name,
            port = url.port,
            password = url.password)
