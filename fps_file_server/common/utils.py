#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
工具
"""
import os
import re
import random
import datetime
import traceback
import shutil


def utime(path, mtime, atime=None):
    """
    修改文件 访问时间与修改时间
    :param path:
    :param atime:最后一次访问时间
    :param mtime:最后一次修改时间
    :return:
    """
    if atime is None:
        atime = mtime
    os.utime(path, (atime, mtime))


def get_new_name_for_time(file_name, use_salt=True, salt_num=3):
    """
    获取一个 时间相关 新文件名
    :param file_name:
    :param use_salt: 是否用盐
    :return:
    """
    start_name, end_name = os.path.splitext(file_name)  # 文件名 与 后缀
    now = datetime.datetime.now()
    time_name = datetime.datetime.strftime(now, "%Y%m%d-%H%M%S")
    salt = get_random_string(salt_num)
    if use_salt:
        # 原名 + 时间 + 随机值 + 原文件后缀
        name = start_name + '-' + time_name + '-' + salt + end_name
    else:
        # 原名 + 时间 + 原文件后缀
        name = start_name + '-' + time_name + end_name
    return name


def get_new_file_name(file_name, fu_str='-副本'):
    """
    获取一个 新文件名
    :param file_name: ‘’text1-副本1.txt‘’
    :param fu_str: 副本默认尾椎
    :return:  ‘’text1-副本2.txt‘’
    """
    start_name, end_name = os.path.splitext(file_name)  # 文件名 与 后缀
    new_start_name = start_name + '{}1'.format(fu_str)  # 默认新名
    pattern = re.compile(r'({}?)(\d+?)$'.format(fu_str))  # r'(-副本?)(\d+?)$'
    searchObj = pattern.search(start_name)
    if not searchObj:  # 没有匹配出来返回默认
        return new_start_name + end_name
    fu = searchObj.group(1)  # -副本 字符串
    fu_num = searchObj.group(2)  # 副本数
    if fu and fu_num:
        fu_num = int(fu_num)
        fu_num += 1
        fu_num = str(fu_num)
        new_start_name = re.sub(r'\d+?$', fu_num, start_name)

    # 新名 + 原文件后缀
    return new_start_name + end_name


# 得到随机字符串
def get_random_string(cnt, num=False):
    """
    :param cnt: 随机字符串的位数
    :param num: 是否纯数字
    :return:    返回cnt长度的随机字符串
    """

    num_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    letter_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                   'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                   'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                   'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    if num:
        string_list = num_list
    else:
        string_list = num_list + letter_list

    result = ''
    for i in range(cnt):
        result = result + random.choice(string_list)

    return result


# 在字典列表里，根据key和val取出这个字典项
# dict_list = [{'id': 1}, {'id': 2}, {'id': 3}]
# get_item_in_list(dict_list, 'id', 2) 得到 {'id': 2}
def get_item_in_list(dict_list, key, value):
    for item in dict_list:
        if value == item[key]:
            return item
    return {}


# 列表转字符串
def list_to_string(in_list, sep=','):
    result = ''
    try:
        if 'list' in str(type(in_list)):
            for item in in_list:
                result = result + str(item) + sep

        result = result.strip(sep)
    except:
        print(traceback.format_exc())

    return result


# 判断是否手机
def is_phone(instr):
    result = True
    try:
        instr = str(instr)

        if len(instr) != 11 or (not instr.isdigit()) or (not instr.startswith('1')):
            result = False
    except:
        pass

    return result


def is_email(str):
    result = False

    if re.match(r"^(\w)+((\w)*(\.|\-)*(\w)*)*(\w)+@(\w)+(\-)*(\w)+((\.\w+)+)$", str):
        result = True
    return result


# 判断是否数字(整型或者浮点)
def is_digit(instr):
    result = False
    try:
        instr = str(instr)
        patten = re.compile(r'^(\-|\+)?\d+(\.\d+)?$')
        if patten.match(instr):
            result = True
    except:
        pass
    return result


# 判断是否整数
def is_int(instr):
    result = False
    try:
        instr = str(instr)
        patten = re.compile(r'^[-+]?[0-9]+$')
        if patten.match(instr):
            result = True
    except:
        pass
    return result


def is_date(value):
    """判断字符串是否日期格式
    :param value:
    :return:
    """
    try:
        value_date = datetime.datetime.strptime(value, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def is_time(value):
    """判断字符串是否时间格式
    :param value:
    :return:
    """
    try:
        value_date = datetime.datetime.strptime(value, '%H:%M:%S')
        return True
    except ValueError:
        return False


def is_datetime(value):
    """判断字符串是否日期时间格式
    :param value:
    :return:
    """
    try:
        value_date = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False


def get_sort_info(sort):
    """ 排序信息
        sort=price
        sort=-price

    :returns: -,price
    """

    if not sort:
        return None, None
    if sort[0] == '-':
        sort_field = sort[1:]
        sort_func = '-'
    else:
        sort_field = sort
        sort_func = '+'
    return sort_field, sort_func


def get_beijing_time(time_str):
    pattern = re.compile('T')
    has_t = pattern.search(time_str)

    if has_t:
        utcTime = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        localtime = utcTime + datetime.timedelta(hours=8)  # 北京时间
    else:
        if len(time_str) == 10:
            localtime = datetime.datetime.strptime(time_str, '%Y-%m-%d')
        elif len(time_str) == 19:
            localtime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        else:
            localtime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
    return localtime


try:
    import psutil
except:
    pass


def get_proc_info(_pid=None):
    """获取进程信息"""
    if _pid is None:
        _pid = os.getpid()
    proc = psutil.Process(_pid)
    proc_info = dict()
    with proc.oneshot():
        proc_info["pid"] = proc.pid
        proc_info["ppid"] = proc.ppid()
        proc_info["进程名"] = proc.name()
        proc_info["拥有该进程的用户"] = proc.username()
        proc_info["可执行绝对路径"] = proc.cwd()
        proc_info["进程的CPU占用率"] = proc.cpu_percent()
        mem = proc.memory_info()
        proc_info["mem_info"] = str(mem)
        proc_info["mem_rss(MB)"] = mem.rss / (1024 ** 2)
        proc_info["进程的内存占用率"] = proc.memory_percent()
        proc_info["线程数"] = proc.num_threads()
        proc_info["进程的优先级"] = proc.nice()
    return proc_info
