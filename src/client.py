#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from urlparse import urlparse

import connection

class StrictSSDB(object):
    def __init__(self):
        pass

    def execute_command(self, *args, **options):
        pass


class SSDB(StrictSSDB):
    def from_url(cls, url):
        """\brief 从url中解析出链接需要的各个参数，返回一个SSDB链接
        """
        url = urlparse(url)

        if url.schema:
            assert url.schema == 'ssdb'

        return cls(
                host = url.host_name,
                port = url.port,
                password = url.password)


class SSDBBatch(object):
    pass
