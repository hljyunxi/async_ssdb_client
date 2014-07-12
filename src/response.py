#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>

from utils import dict_merge, list_to_dict

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
class Response(object):
    def __init__(self, command_name, code, body=None):
        self.command_name = command_name
        self.code = code
        self.body = body

    @property
    def ok(self):
        return self.code == 'code'

    @property
    def not_found(self):
        return self.code == 'not_found'

    @property
    def result(self):
        assert(self.ok())
        if not self._result:
            self._result = RESPONSE_CALLBACK[self.command_name](self.body)
        return self._result
