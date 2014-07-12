#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

class Response(object):
    def __init__(self, code, body=None):
        self.code = code
        self.body = body
        print code, body

    @property
    def ok(self):
        return self.code == 'code'

    @property
    def not_found(self):
        return self.code == 'not_found'


