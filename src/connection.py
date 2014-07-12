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

from iostream import IOStream
from response import Response


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

    def set_request_callback(self, request_callback):
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
        tobe_pack_command = list(largs)
        tobe_pack_command.insert(0, command_name)
        self.send_packed_command(self.pack_command(*tobe_pack_command))

    def send_packed_command(self, command):
        if not self._alive:
            self._connect()

        self._io_stream.write(command)
        if self._request_callback:
            self._io_stream.read_until("\n", self._parse_line_length)


    def pack_command(self, *largs):
        args = chain(*[(str(len(str(i))), str(i)) for i in largs])
        sep = '\n'
        return sep.join(args)+sep*2

    def _internal_connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._io_stream = IOStream(sock)
        self._io_stream.connect((self.host, self.port), self._connect_callback)
        self._io_stream.set_close_callback(self._socket_close_callback)

    def _connect_callback(self):
        pass

    def _parse_line_length(self, data):
        if data == '\n':
            response = Response(self._tmp_response[0], self._tmp_response[1:])
            self._tmp_response = []
            self._invoke_response_callback(response)
        else:
            try:
                line_length = int(data[:-1])
            except Exception, e:
                self._tmp_response = []
                self._invoke_repsonse_callback(None, error=e)

            self._io_stream.read_bytes(line_length + 1, self._parse_line_content)

    def _parse_line_content(self, data):
        self._tmp_response.append(data[:-1])
        self._io_stream.read_until('\n', self._parse_line_length)

    def _invoke_response_callback(self, response, error=None):
        callback = self._request_callback
        self._request_callback = None
        callback(response, error=error)


class UnixDomainSocketConnection(Connection):
    def __init__(self, path='', encoding='UTF-8'):
        self.path = path
        self.encoding = encoding
        self._stream = None
        self._request_callback = None
        self._alive = False
        self._tmp_response = []

    def _internal_connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        return sock
