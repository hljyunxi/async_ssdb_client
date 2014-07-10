#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

import os

from threading import Lock, Condition
from itertools import chain

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

import socket


class Connection(object):
    def __init__(self, pool, host='localhost', port=6379, encoding='UTF-8'):
        self.pool = pool
        self.host = host
        self.port = port
        self.encoding = encoding
        self._stream = None
        self._request_callback = None
        self._alive = False

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def set_request_callback(self, rquest_callback):
        assert !self._request_callback, "already in use, _request_callback is not None"
        self._request_callback = request_callback

    def connect(self):
        if self._alive:
            return

        try:
            self._connect()
        except socket.error, e:
            raise e

        self._alive = True

    def _socket_close_callback(self):
        """\brief used in io_stream close callback
        """
        callback = self._request_callback
        self._request_callback = None

        try:
            if callback:
                callback(None, InterfaceError('connection error'))
        finally:
            self._active = False
            self.pool.release_connection(self)

    def close(self):
        try:
            self._close()
        finally:
            self._pool.release_connection(self)

    def _close(self):
        callback = self._request_callback
        self._request_callback = None

        try:
            if callback:
                callback(None, InterfaceError('connection close'))
        finally:
            self._active = False
            self._io_stream.close()


    def send_command(self, *largs):
        self.send_packed_command(self.pack_command(*largs))

    def send_packed_command(self, command):
        if not self._alive:
            self.connect()

        self._io_stream.write(command)
        if self._request_callback:
            self._io_stream.read_until_regex(r"\r\n", self._parse_response)
        else:
            self.close()

    def pack_command(self, *largs):
        args = chain(*[(str(len(str(i))), str(i)) for i in largs])
        sep = '\n'
        return sep.join(args)+sep*2

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._io_stream = IOStream(sock)
        self._io_stream.connect((self.host, self.port), self._connect_callback)
        self._io_sream.set_close_callback(self._socket_close_callback)

    def _connect_callback(self):
        pass

    def _close_callback(self):
        self._alive = False
        self.pool.release_connection(self)

    def _parse_response(self, data):
        pass


class UnixDomainSocketConnection(Connection):
    def __init__(self, pool, path='', encoding='UTF-8',
            decoder_class=ConnectionParser):
        self.pool = pool
        self.path = path
        self._decoder = decoder_class()
        self.encoding = encoding
        self._sock = None

    def _connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        return sock
