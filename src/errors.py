#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>


class SSDBError(Exception):
    pass

class ConnectionError(SSDBError):
    pass

class InvalidResponse(SSDBError):
    pass

class DataError(SSDBError):
    pass

class ResponseError(SSDBError):
    pass

class InterfaceError(SSDBError):
    pass
