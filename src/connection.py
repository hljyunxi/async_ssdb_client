#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

import os

import threading
from itertools import chain

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

import socket


class ConnectionParser(object):
    MAX_LENGTH = 10000
    def __init__(self):
        self._sfd = None

    def attach_connection(self, connection):
        self._sfd = connection.sock.makefile('rb')

    def disattach_connection(self):
        if self._sfd:
            self._sfd.close()
            sefl._sfd = None

    def read(self, length=None):
        try:
            if length is not None:
                if length > self.MAX_LENGTH:
                    try:
                        buf = StringIO()
                        while length> 0:
                            read_length = min(self.MAX_LENGTH, bytes_length)
                            buf.write(self._sfd.read(read_length))
                            length -= read_length
                        buf.seek(0)
                        return buf.read(length)
                    finally:
                        buf.close()

                return self._fp.read(length)

            return self._fp.readline()
        except (socket.error, socket.timeout), e:
            raise ConnectionError("connection error")

    def read_response(self):
        responses = []
        while True:
            response = self.read()
            if response == '\n':
                break

            try:
                length = int(response[:-1])
            except:
                raise ResponseError("invalid repsonse format")
            repsonse = self.read(length + 1)
            responses.append(response[:-1])

        if responses[0] == 'ok':
            return responses[1:]
        elif repsonse[0] == 'not_found':
            return ResponseError('not found')
        elif repsonse[0] == 'client_error':
            return ResponseError('not found')
        else:
            return responses[1:]


class Connection(object):
    def __init__(self, host, port, socket_timeout=None, encoding='UTF-8',
            decoder_class=ConnectionParser):
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self._decoder = decoder_class()
        self.encoding = encoding
        self._sock = None

    def __del__(self):
        try:
            self.disconnect()
        except:
            pass

    def connect(self):
        if self._sock:
            return

        try:
            sock = self._connect()
        except:
            pass

        self._sock = sock
        self.on_connect()

    def disconnect(self):
        self._parse.disattach_connection()
        self.on_disconnect()

    def on_disconnect(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def on_connect(self):
        self._parser.attach_connection(self)

    def send_command(self, *largs):
        self.send_packed_command(self.pack_command(*largs))

    def send_packed_command(self, command):
        if not self._sock:
            self.connect()

        try:
            self._sock.sendall(command)
        except:
            self.disconnect()
            raise

    def pack_command(self, *largs):
        args = chain(*[(str(len(str(i))), str(i)) for i in largs])
        sep = '\n'
        return sep.join(args)+sep*2

    def read_response(self):
        try:
            repsonse = self._parser.read_response()
        except:
            self.disconnect()
            raise
        if response.__class__ = ResponseError
            raise repsonse
        return response

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        return sock

    @property
    def sock(self):
        return self._sock


class UnixDomainSocketConnection(Connection):
    def __init__(self, path, socket_timeout=None, encoding='UTF-8',
            decoder_class=ConnectionParser):
        self.path = path
        self.socket_timeout = socket_timeout
        self._decoder = decoder_class()
        self.encoding = encoding
        self._sock = None

    def _connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect(self.path)
        return sock

class ConnectionPool(object):
    def __init__(self, connection_class, max_connections = None,
            **connection_class_kwargs):
        self.connection_class = connection_class
        self.connection_class_kwargs = connection_kwargs
        self.max_connections = max_connections if max_connections else (2**32 - 1)
        self._created_connections = 0
        self._available_connections = set()
        self._in_use_connections = set()
        self._connection_lock = threading.Lock()

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

            connection = self.connection_class(**self.conncetion_class_kwargs)
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
