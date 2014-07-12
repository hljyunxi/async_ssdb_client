#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from urlparse import urlparse

from connection import Connection, UnixDomainSocketConnection
from utils import dict_merge, list_to_dict
from functools import partial
from connection_pool import ConnectionPool

class StrictSSDB(object):
    RESPONSE_CALLBACK = dict_merge(
        list_to_dict(
            'set',
            lambda r: bool(int(r[0]))
        ),
        list_to_dict(
            'get',
            lambda r: r[0]
        ),
    )
    def __init__(self, host='127.0.0.1', port=8888, connection_pool=None,
            encoding='UTF-8', unix_domain_path=None):
        if not connection_pool:
            connection_pool_args = {
                'encoding': encoding,
            }

            if unix_domain_path:
                connection_pool_args.update({
                    'path': unix_domain_path,
                    'connection_class': Connection,
                })
            else:
                connection_pool_args.update({
                    'host': host,
                    'port': port,
                    'connection_class': UnixDomainSocketConnection,
                })
            connection_pool = ConnectionPool(**connection_pool_args)

        self.connection_pool = connection_pool
        self.response_callback = self.__class__.RESPONSE_CALLBACK.copy()

    def set_response_callback(self, command, callback):
        self.response_callback[command] = callback

    def execute_command(self, command_name, *largs, **kwargs):
        connection = self.connection_pool.get_connection(command_name, **kwargs)
        future = Future()
        try:
            connection.set_requst_callback(partial(self._handle_response,
                future=future, command_name=command_name))
            connection.send_command(command_name, *largs)
        except IOError, e:
            connection.close()

        return future

    def _handle_response(self, response, command_name=None, future=None,
            error=None):
        future.set_response(response)
        self.connection_pool.release_connection(conncetion)


class SSDB(StrictSSDB):
    """\brief 一些辅助性工具

        比如:
        1.从url中构建链接
        2.batch的创建
    """
    @classmethod
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

    def batch(self):
        pass

class SSDBBatch(object):
    pass
