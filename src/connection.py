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
    """\biref ssdb连接的封装
    iostream帮我们处理了socket.error的情况, 但是IOError以及connection中自己
    触发的error需要应用程序来处理
    """
    def __init__(self, host='localhost', port=6379, encoding='UTF-8'):
        self.host = host
        self.port = port
        self.encoding = encoding
        self._stream = None
        self._request_callback = None
        self._alive = False
        self._tmp_response = []

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def set_request_callback(self, rquest_callback):
        assert not self._request_callback, "already in use, _request_callback is not None"
        self._request_callback = request_callback

    def _connect(self):
        if self._alive:
            return

        try:
            self._internal_connect()
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
                callback(None, ConnectionError('connection error'))
        finally:
            self._active = False

    def close(self):
        callback = self._request_callback
        self._request_callback = None

        try:
            if callback:
                callback(None, ConnectionError('connection close'))
        finally:
            self._active = False
            self._io_stream.close()


    def send_command(self, command_name, *largs):
        self.send_packed_command(self.pack_command(*largs))

    def send_packed_command(self, command):
        if not self._alive:
            self._connect()

        self._io_stream.write(command)
        if self._request_callback:
            self._io_stream.read_until_regex(r"\r\n", self._parse_response)


    def pack_command(self, *largs):
        args = chain(*[(str(len(str(i))), str(i)) for i in largs])
        sep = '\n'
        return sep.join(args)+sep*2

    def _internal_connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._io_stream = IOStream(sock)
        self._io_stream.connect((self.host, self.port), self._connect_callback)
        self._io_sream.set_close_callback(self._socket_close_callback)

    def _connect_callback(self):
        pass


    def _parse_response(self, data):
        if data == '\n' or data == '\r\n':
            callback = self._request_callback
            self._request_callback = None

            repsonse = Response(self._tmp_response[0], self._tmp_response[1:])
            self._tmp_response = []

            callback(repsonse)
        else:
            self._tmp_response.append(data[:-2] if data.endswith('\r\n') else data[:-1])
            self.io_stream.read_until_regex('\r?\n', self._parse_response)


class UnixDomainSocketConnection(Connection):
    def __init__(self, path='', encoding='UTF-8'):
        self.path = path
        self._decoder = decoder_class()
        self.encoding = encoding
        self._sock = None

    def _internal_connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        return sock
