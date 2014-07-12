#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>
import ioloop
from client import SSDB

ssdb_client = SSDB()
set_result = ssdb_client.execute_command('set', 'a', '1224')
#get_result = ssdb_client.execute_command('get', 'a')
ioloop.IOLoop.instance().start()
