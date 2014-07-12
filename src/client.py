#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from urlparse import urlparse

from connection import Connection, UnixDomainSocketConnection
from functools import partial
from connection_pool import ConnectionPool
from future import AsyncResult

class StrictSSDB(object):
    def __init__(self, host='127.0.0.1', port=8888, connection_pool=None,
            encoding='UTF-8', unix_domain_path=None):
        if not connection_pool:
            connection_pool_args = {
                'encoding': encoding,
            }

            if unix_domain_path:
                connection_pool_args.update({
                    'path': unix_domain_path,
                    'connection_class': UnixDomainSocketConnection,
                })
            else:
                connection_pool_args.update({
                    'host': host,
                    'port': port,
                    'connection_class': Connection,
                })
            connection_pool = ConnectionPool(**connection_pool_args)

        self.connection_pool = connection_pool

    def execute_command(self, command_name, *largs, **kwargs):
        connection = self.connection_pool.get_connection(command_name, **kwargs)
        async_result = AsyncResult()
        connection.set_request_callback(partial(self._handle_response,
            async_result=async_result, command_name=command_name,
            connection=connection))

        try:
            connection.send_command(command_name, *largs)
        except:
            connection.close()
            self.connection_pool.release_connection(connection)
            raise

        return async_result

    def _handle_response(self, response, command_name=None, async_result=None,
            error=None, connection=None):
        if response:
            async_result.set(value=response)
        elif error:
            connection.close()
            async_result.set_exception(error)

        self.connection_pool.release_connection(connection)


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
