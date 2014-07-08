#!/usr/bin/python
#coding: utf8
#Author: chenyunyun<hljyunxi@gmail.com>


#公用utils

def dict_merge(*dicts):
    merged_dicts = {}
    [merged_dicts.update(d) for d in dicts]
    return merged_dicts



