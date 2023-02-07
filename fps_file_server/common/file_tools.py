#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import re
import os
import shutil
import sys
import time
import json
import zipfile
import codecs
import pathlib
import platform
import ctypes
import subprocess
import shutil
import base64
import errno

import psutil

from fps_file_server.common.mine_types import MINE_TYPES
from fps_file_server.exceptions import logger
from fps_file_server.exceptions import FileServerError as Error
from fps_file_server.config import Config


def fsync_dir(dir_path):
    """
    Execute fsync on a directory ensuring it is synced to disk

    :param str dir_path: The directory to sync
    :raise OSError: If fail opening the directory
    """
    dir_fd = os.open(dir_path, os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    except OSError as e:
        # On some filesystem doing a fsync on a directory
        # raises an EINVAL error. Ignoring it is usually safe.
        if e.errno != errno.EINVAL:
            raise
    finally:
        os.close(dir_fd)


def linux_exists(path):
    """
    查看文件存在 linux原生命令
    :param path: 绝对路径
    :return:
    """
    cmds = ['find', path]
    print(cmds)
    p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if out:
        return True
    else:
        return False


def nfs_flush(path):
    """nfs 刷新缓存"""
    try:
        dirname = os.path.dirname(path)
        dstat = os.stat(dirname)
        os.chown(dirname, dstat.st_uid, dstat.st_gid)
    except Exception as e:
        logger.error(e)


def exists(path):
    """
    路径是否存在
    :param path: 绝对路径
    :return:
    """
    if 'linux' in sys.platform:  # linux原生命令对nfs性能更好
        nfs_flush(path)
        return os.path.exists(path)
    else:
        return os.path.exists(path)


def is_writable(path):
    """
    是否可写
    :param path：实际地址
    :return:
    """
    return os.access(path, os.W_OK)


def is_readable(path):
    """
    是否可读
    :param path: 实际地址
    :return:
    """
    return os.access(path, os.R_OK)


def is_executable(path):
    """
    是否可执行
    :param path: 实际地址
    :return:
    """
    return os.access(path, os.X_OK)


def check_child_path(child, parents):
    """
    child是否是parents 的 子孙路径
    :param child: 子孙路径
    :param parents: 祖宗路径
    :return:
    """
    return child.startswith(os.path.abspath(parents) + os.sep)


def copy_dir(yuan, target):
    """
    将一个目录下的全部文件和目录,完整地<拷贝并覆盖>到另一个目录
    @param yuan: 源目录
    @param target: 目标目录
    @return:
    """
    if 'linux' in sys.platform:  # linux原生命令性能更好
        return copydir_by_linux(yuan, target)

    # 源路径必须是目录
    if not os.path.isdir(yuan):
        raise Error('源目录 不是文件夹')
    # 如果目标路径存在且不是目录 就结束
    if exists(target):
        if not os.path.isdir(target):
            raise Error('目标粘贴路径存在同名文件')
    else:
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)

    for path, dirnames, filenames in os.walk(yuan):
        # 递归创建目录
        for d in dirnames:
            dir_path = os.path.join(path.replace(yuan, target), d)
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)
        # 递归拷贝文件
        for f in filenames:
            dep_path = os.path.join(path, f)
            arr_path = os.path.join(path.replace(yuan, target), f)
            copyfile(dep_path, arr_path)


def move_dir(yuan, target):
    """
    将一个目录下的全部文件和目录,完整地<拷贝并覆盖>到另一个目录
    @param yuan: 源目录
    @param target: 目标目录
    @return:
    """

    # 源路径必须是目录
    if not os.path.isdir(yuan):
        raise Exception('源目录 不是文件夹')
    # 如果目标路径存在且不是目录 就结束
    if exists(target):
        if not os.path.isdir(target):
            raise Error('目标粘贴路径存在同名文件')
    else:
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)

    for path, dirnames, filenames in os.walk(yuan):
        # 递归创建目录
        for d in dirnames:
            dir_path = os.path.join(path.replace(yuan, target), d)
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)
        # 递归拷贝文件
        for f in filenames:
            dep_path = os.path.join(path, f)
            arr_path = os.path.join(path.replace(yuan, target), f)
            print(dep_path, arr_path)
            shutil.move(dep_path, arr_path)
    shutil.rmtree(yuan)


def timeout_file_cleanup(path, days=3):
    """

    :param path:
    :param days: 超时天数 例： days=3 删除超时3天以上的文件
    :return:
    """
    if not exists(path):
        return
    if os.path.isdir(path):
        return
    last_modified = os.path.getmtime(path)  # 文件最后更新时间时间戳
    modified_time = datetime.datetime.fromtimestamp(last_modified)  # datetime
    three_day_ago = datetime.datetime.now() - datetime.timedelta(days=days)  # datetime
    if modified_time < three_day_ago:
        os.remove(path)
        return path


def timeout_file_cleanup_for_dir(dir_path, days=3):
    """
    清空目录下的 超时文件
    :param path:
    :param days: 超时天数 例： days=3 删除超时3天以上的所有文件
    :return:
    """
    if not os.path.isdir(dir_path):
        raise Error('func:timeout_file_cleanup_for_dir path不是文件夹')
    clean_paths = []
    for path, dirnames, filenames in os.walk(dir_path):  # 遍历所有子目录
        for filename in filenames:
            file_path = os.path.join(path, filename)
            clean_path = timeout_file_cleanup(file_path, days=days)
            if clean_path:
                clean_paths.append(clean_path)
    return clean_paths


def get_path_size_by_linux(path: str):
    """
    利用 linux 原生命令 查看文件(夹)大小
    @param path:
    @return: @int size 字节
    """

    def du_sb():
        context = None
        size = None
        cmds = ['du', '-sb', path]
        p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            context = line.decode()

        if context:
            pattern = re.compile(r'^(\d+)')
            searchObj = pattern.search(context)
            size_str = searchObj.group(1)
            size = int(size_str)
        return size

    def du_sk():
        context = None
        size = None
        cmds = ['du', '-sk', path]
        p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            context = line.decode()

        if context:
            pattern = re.compile(r'^(\d+)')
            searchObj = pattern.search(context)
            size_str = searchObj.group(1)
            size = int(size_str)
        return size * 1024  # kb转b

    size = du_sb()
    if size is None:
        size = du_sk()
    return size


def copyfile_by_linux(path, new_path):
    cmds = ['cp', path, new_path]
    p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
    out, err = p.communicate()


def copydir_by_linux(path, new_path):
    cmds = ['cp', '-r', path, new_path]
    p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
    out, err = p.communicate()


def copyfile(path, new_path):
    if 'linux' in sys.platform:  # linux原生命令性能更好
        return copyfile_by_linux(path, new_path)
    else:
        shutil.copyfile(path, new_path)


def get_path_size(path):
    """
    根据路径获取 文件/文件夹的大小
    :param path:
    :return: dir_size 字节
    """
    if os.path.isfile(path):
        return os.path.getsize(path)
    if 'linux' in sys.platform:  # linux原生命令性能更好
        _size = get_path_size_by_linux(path)
        if _size:
            return _size

    dir_size = 0
    for _path, dirnames, filenames in os.walk(path):  # 遍历所有子目录
        for filename in filenames:
            file_path = os.path.join(_path, filename)
            try:
                size = os.path.getsize(file_path)
            except:
                size = 0
            dir_size += size
    return dir_size


def get_free_space_mb(folder):
    """
    获取当前目录所在磁盘 剩余空间
    :param folder: 磁盘路径 例如 D:\\
    :return: 剩余空间 单位 字节
    """
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
        return free_bytes.value / 1024
    else:
        st = os.statvfs(folder)
        return st.f_bavail * st.f_frsize


def get_blocks_space_mb(folder):
    """获取当前目录所在磁盘 全部空间"""
    st = os.statvfs(folder)
    return st.f_blocks * st.f_frsize


def get_pvc_info(folder):
    """
    当前目录所在磁盘信息
    :param folder:
    :return: {'percent': 已用空间率, 'used': 已用空间, 'total': 总空间,
            'free': 可用空间}
    """
    disk_info = psutil.disk_usage(folder)
    return {'percent': disk_info.percent, 'used': disk_info.used, 'total': disk_info.total,
            'free': disk_info.free}


def zip_dir(dirpath, out_path):
    """
    压缩指定文件夹 到指定路径.zip
    :param dirpath: 文件夹路径 dirpath : '/home/xiang/workproject/jpt_filesystem/static/log'
    :param out_path: 导出路径 '/home/xiang/workproject/jpt_filesystem/tests/log.zip'
    :return: 无
    """
    _diranme = os.path.dirname(dirpath)
    dir_dir_path = os.path.dirname(dirpath)  # 压缩文件夹的父目录

    zip = zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED)
    for path, dirnames, filenames in os.walk(dirpath):  # 遍历所有子目录
        # 去掉目标根路径，只对目标文件夹下边的文件及文件夹进行压缩
        z_dir_path = path.replace(dir_dir_path, '')  # 去掉父目录 获取相对目录
        for filename in filenames:
            file_path = os.path.join(path, filename)  # 文件在外面的实际路径
            z_file_path = os.path.join(z_dir_path, filename)  # 在zip内的相对路径
            zip.write(file_path, z_file_path)
    zip.close()
    return out_path


def zip_dir_bytes(dirpath, zip_bytes_iO):
    """
    压缩指定文件夹 到内存
    :param dirpath:实际文件夹路径 dirpath : '/home/xiang/workproject/jpt_filesystem/static/log'
    :param zip_bytes_iO: BytesIO  例子  memory_file = BytesIO()
    :return: BytesIO
    """
    _diranme = os.path.dirname(dirpath)
    dir_dir_path = os.path.dirname(dirpath)  # 压缩文件夹的父目录

    zip = zipfile.ZipFile(zip_bytes_iO, "w", zipfile.ZIP_DEFLATED)
    for path, dirnames, filenames in os.walk(dirpath):  # 遍历所有子目录
        # 去掉目标根路径，只对目标文件夹下边的文件及文件夹进行压缩
        z_dir_path = path.replace(dir_dir_path, '')  # 去掉父目录 获取相对目录
        for filename in filenames:
            file_path = os.path.join(path, filename)  # 文件在外面的实际路径
            z_file_path = os.path.join(z_dir_path, filename)  # 在zip内的相对路径
            with open(file_path, 'rb') as fp:
                content = fp.read()
                print(z_file_path)
                zip.writestr(z_file_path, content)
    zip.close()
    return zip_bytes_iO


def read_file(filePath, encoding="gbk"):
    """读文件"""
    with codecs.open(filePath, "r", encoding) as f:
        return f.read()


def write_file(filePath, u, encoding="utf-8"):
    """写文件"""
    with codecs.open(filePath, "w", encoding) as f:
        f.write(u)


def gbk_to_utf8(path, newpath):
    """编码转换 gbk>utf8"""
    content = read_file(path, encoding="gbk")
    write_file(newpath, content, encoding='utf-8')


def check_file_name_length_available(file_name: str):
    """检查文件名长度是否可用"""
    return len(file_name.encode('utf-8')) <= 255


def root_path_change(root_path, path, is_exists=None, no_exists=None, isdir=None, isfile=None):
    if not path.startswith('/'):
        raise Error('path:{} 不合法 必须要/'.format(path))

    old_path = path
    if path.startswith('/'):
        path = path[1:]
        if path.startswith('/'):  # 两条斜杠不合法
            raise Error('path:{} 不合法'.format(old_path))

    _path = os.path.join(root_path, path)

    # 校验
    if is_exists:
        if not exists(_path):
            logger.error('不存在：{}'.format(_path))
            raise Error('不存在：{}'.format(path), code=404)
    if no_exists:  # 要求它不存在
        if os.path.isdir(_path):
            logger.debug('is exists dir : {}'.format(_path))
            raise Error("文件夹名称已存在")
        elif os.path.isfile(_path):
            logger.debug('is exists file : {}'.format(_path))
            raise Error("文件名称已存在")

    if isdir:
        if not os.path.isdir(_path):
            raise Error("这不是一个目录：{}".format(path))
    if isfile:
        if os.path.isdir(_path):
            raise Error("这是一个目录，不是文件：{}".format(path))

    return _path


def check_file_too_large(path, max_size):
    """
    检查文件（夹）大小
    :param path:
    :param max_size:
    :return:
    """
    if not exists(path):
        return False
    if os.path.isfile(path):
        if os.path.getsize(path) > max_size:
            return True
    size = 0
    for path, dirnames, filenames in os.walk(path):
        # 递归查询文件大小
        for f in filenames:
            _path = os.path.join(path, f)
            try:
                _size = os.path.getsize(_path)
            except:
                _size = 0
            size += _size
            if size > max_size:
                return True


def check_file_too_many(path, max_num):
    """检查文件数量 （非常节约性能）"""
    if not exists(path):
        return False
    if os.path.isfile(path):
        return False
    num = 0
    for path, dirnames, filenames in os.walk(path):
        num += len(filenames)
        if num > max_num:
            return True


def get_new_untitled_name(file_name):
    start_name, end_name = os.path.splitext(file_name)  # 文件名 与 后缀
    if start_name == Config.UNTITLED_NAME:
        return Config.UNTITLED_NAME + '1' + end_name
    pattern = re.compile(Config.UNTITLED_COMPILE)  # r'(-副本?)(\d+?)$'
    searchObj = pattern.search(start_name)
    if not searchObj:  # 没有匹配出来抛异常  结束外面的while True
        raise Error('Untitled 文件名生成失败')
    fu = searchObj.group(1)  # -副本 字符串
    fu_num = searchObj.group(2)  # 副本数
    if fu and fu_num:
        fu_num = int(fu_num)
        fu_num += 1
        fu_num = str(fu_num)
        new_start_name = re.sub(r'\d+?$', fu_num, start_name)
        # 新名 + 原文件后缀
        return new_start_name + end_name
    raise Error('Untitled 文件名生成失败')


def get_suffix(file_name: str) -> str:
    """获取文件后缀"""
    suffix = ''
    split = file_name.split(".")
    if len(split) > 1:
        suffix = split[-1]
    return suffix


def get_mimetype(file_name: str):
    """
    获取文件 mime type
    """
    suffix = get_suffix(file_name)
    mimetype = MINE_TYPES.get(suffix, '')
    return mimetype


def get_format(mimetype: str):
    if mimetype == MINE_TYPES['ipynb']:
        _format = 'json'
    elif mimetype in [MINE_TYPES['gif'], MINE_TYPES['jpg'], MINE_TYPES['jpeg'], MINE_TYPES['png']]:
        _format = 'images'
    else:
        _format = 'text'
    return _format


def get_file_content(_path, _format, size=None):
    if size is None:
        size = os.path.getsize(_path)
    content = None
    if _format == 'json':
        try:
            with open(_path, "r", encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            raise Error('只支持预览utf-8编码文件')
        try:
            content = json.loads(content)  # json转dict 后面view层return会转json
        except Exception as e:
            raise Error('文件内容不是json:{}'.format(str(e)))
    elif _format == 'images':
        if size < Config.IMAGES_PREVIEW_SIZE:
            with open(_path, "rb") as f:
                content = base64.b64encode(f.read())
    elif _format == 'text':
        if size < Config.TEXT_PREVIEW_SIZE:
            try:
                with open(_path, "r", encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                raise Error('只支持预览utf-8编码文件')
    return content


def check_upload_file(file_name):
    """是否是占位文件"""
    if not file_name[0] == '.':
        return False
    if get_suffix(file_name) == Config.UPLOAD_FILE_EXTENSION:
        return True


def path_legal_verification(path):
    """
    路径合法性校验
    :param path:
    :param limit_root:
    :return:
    """
    # 相对路径校验
    if not isinstance(path, str):
        raise Error('path不能为空，且必须是字符串')
    if not path.startswith('/'):
        raise Error('path不合法：{} 必须要/开头'.format(path))
    splits = path.split('/')
    for split in splits:
        if split in ['.', '..']:
            raise Error('path:{} 不合法'.format(path))
    # 业务校验
    path = os.path.abspath(path)
    return path


def chmod777(path):
    cmds = ['chmod', '777', '-R', path]
    p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
    out, err = p.communicate()


def chmod771(path):
    cmds = ['chmod', '771', '-R', path]
    p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
    out, err = p.communicate()


if __name__ == '__main__':
    pass
