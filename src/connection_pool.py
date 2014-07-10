#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from threading import Lock

class ConnectionPool(object):
    def __init__(self, connection_class, max_connections = None,
            **connection_class_kwargs):
        self.connection_class = connection_class
        self.connection_class_kwargs = connection_kwargs
        self.max_connections = max_connections if max_connections else (2**32 - 1)
        self._created_connections = 0
        self._available_connections = set()
        self._in_use_connections = set()
        self._connection_lock = Lock()

    def get_connection(self):
        try:
            connection = self._available_connection.pop()
        except IndexError:
            connection = self._make_connection()

        return connection

    def _make_connection(self):
        with self._connection_lock:
            if self._created_connections >= self.max_connections:
                raise ConnectionError('too many connections')

            connection = self.connection_class(self, **self.conncetion_class_kwargs)
            self._created_connections += 1
            self._in_use_connections.add(connection)

        return connection

    def release_connection(self, connection):
        with self._connection_lock:
            self._in_use_connections.remove(connection)
            self._available_connections.add(connection)
            self._create_connections -= 1

    def disconnect(self):
        for connection in chain(self._in_use_connections,\
                self._availabe_connections):

            connection.disconnect()
