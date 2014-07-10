#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from urlparse import urlparse

from connection import Connection, UnixDomainSocketConnection
from utils import dict_merge, list_to_dict

class StrictSSDB(object):
    RESPONSE_CALLBACK = utils.dict_merge(
        list_to_dict(
            'set',
            lambda r: bool(int(r[0]))
        ),
        list_to_dict(
            'get',
            lambda r: r[0]
        ),
    )
    def __init__(self, host, port, connection_pool=None, encoding='UTF-8',
            unix_domain_path=None):
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

    def execute_command(self, *largs, **kwargs):
        command_name = largs[0]
        connection = self.connection_pool.get_connection(command_name, **kwargs)
        try:
            connection.send_command(*largs)
        except socket.error, e:
            self.connection_pool.release
        else:
            self.connetion_pool.release_connection(connection)

        return self.parse_response()

    def parse_response(self, connection, command_name, **options):
        repsonse = connection.read_response()
        if command_name in self.response_callback:
            return self.response_callback[command_name](repsonse, **options)
        return response


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
