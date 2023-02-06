#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Title: linux文件操作控制器
@Author：csx
"""
import re
import base64
import time

import pandas as pd
import numpy as np
import pandas.errors
from flask import json, g
import os
import shutil
import datetime
from common.errors import JptFileSystemError, AppCode
from common.conf.config import Config
from common.conf.path import PVC_DIR
from common.constant.mine_types import MINE_TYPES
from common.conf.path import IPYNB_PATH
from common.utils import get_random_string, get_new_file_name
from common.utils.pandas_tools import df_to_dict
from common.utils.decompress_handle import decompress
from common.utils.file_tools import is_writable, is_executable, is_readable, zip_dir, get_path_size, zip_dir_bytes, \
    copy_dir, move_dir, check_file_name_length_available, root_path_change, check_file_too_large, get_new_untitled_name, \
    get_mimetype, get_suffix, check_child_path, get_free_space_mb, check_upload_file, copyfile, path_legal_verification, \
    chmod777, get_format, get_file_content, exists, fsync_dir
from io import BytesIO
from common.log import logger
import pathlib
import portalocker


class FileObjController:
    """文件操作控制器"""

    def __init__(self, root_path, rds=None, experiment_id=None):
        self.ROOT_PATH = root_path
        if rds is None:
            self.rds = g.rds
        else:
            self.rds = rds
        self.experiment_id = experiment_id

    def _init_path(self, path: str, is_exists=None, no_exists=None, isdir=None, isfile=None):
        """
        路径初始化 加入根目录
        :param path: 尾路径
        :param exists: 校验 存在
        :param no_exists: 检验 不存在
        :param isdir: 校验 是否目录
        :param isfile: 校验 是否文件
        :return:
        """
        # 路径重构
        if self.ROOT_PATH is None:  # 如果没有传用户pvc路径，也可当普通工具类用
            return path
        else:
            return root_path_change(self.ROOT_PATH, path, is_exists, no_exists, isdir, isfile)

    def _check_new_file_name(self, file_name):
        if file_name.startswith('/'):
            raise JptFileSystemError("文件名称不合法,不能以'/'或'.' 开头")
        if file_name.startswith('.'):
            raise JptFileSystemError("文件名称不合法,不能以'/'或'.' 开头")
        # # 校验纯空格文件
        # ste = re.sub(r"[ ]", "", file_name)
        # if len(ste) == 0:
        #     raise JptFileSystemError("文件名不合法:不能创建纯空格文件")
        # 检查文件名长度是否可用
        if not check_file_name_length_available(file_name):
            raise JptFileSystemError('文件名称最长64个字符')  # 响应信息是产品要求

    def _check_new_path(self, path):
        file_name = os.path.basename(path)
        self._check_new_file_name(file_name)

    def _get_new_duplicate_path(self, path):
        """获取新的文件副本名和路径"""
        parent_path = os.path.dirname(path)  # 根路径
        name = os.path.basename(path)  # 文件名
        new_name = get_new_file_name(name)
        new_path = os.path.join(parent_path, new_name)
        while exists(new_path):  # 直到不存在
            new_name = get_new_file_name(new_name)
            new_path = os.path.join(parent_path, new_name)
        if not check_file_name_length_available(new_name):
            raise JptFileSystemError('文件名称最长64个字符')
        return new_path, new_name

    def check_exists_in_folder(self, file_name, parent_path):
        """
        检查目录下是否存在同名文件或文件夹
        :param file_name:
        :param parent_path:
        :return:
        """
        path = os.path.join(parent_path, file_name)
        path = path_legal_verification(path)
        _path = self._init_path(path)

        has_same_file = 0
        has_same_folder = 0
        if exists(_path):
            if os.path.isdir(_path):
                has_same_folder = 1
            else:
                has_same_file = 1
        return {'hasSameFile': has_same_file, 'hasSameFolder': has_same_folder}

    def create_folder(self, path, paste_type='duplicate'):
        """创建目录"""
        path = path_legal_verification(path)
        self._check_new_path(path)
        _path = self._init_path(path)

        _new_path = _path  # 实际路径
        new_path = path  # 相对路径
        if paste_type == 'duplicate':
            while exists(_new_path):
                _new_path, new_name = self._get_new_duplicate_path(_new_path)  # 新实际路径
                new_path = os.path.join(os.path.dirname(path), new_name)  # 新相对路径

        pathlib.Path(_new_path).mkdir(parents=True, exist_ok=True)  # 创建目录
        chmod777(_new_path)
        fsync_dir(_new_path)
        info = self.file_contents(new_path, not_get_content=True)
        return info

    def add(self, parent_path):
        """
        新建 空文件
        :param parent_path: 目录
        :return:
        """
        parent_path = path_legal_verification(parent_path)
        _path = self._init_path(parent_path, is_exists=True, isdir=True)
        file_name = Config.UNTITLED_NAME
        _new_path = os.path.join(_path, file_name)
        while exists(_new_path):
            file_name = get_new_untitled_name(file_name)
            _new_path = os.path.join(_path, file_name)
        new_path = os.path.join(parent_path, file_name)

        if 4 * 1024 * 1024 > get_free_space_mb(self.ROOT_PATH):
            raise JptFileSystemError('剩余空间不足')
        file = open(_new_path, 'w')
        file.flush()
        os.fsync(file.fileno())
        file.close()
        chmod777(_new_path)
        return self.file_contents(new_path, not_get_content=True)

    def add_ipynb(self, parent_path):
        """
        新增默认ipynb
        :param path:目录
        :return:
        """
        parent_path = path_legal_verification(parent_path)
        _path = self._init_path(parent_path, is_exists=True, isdir=True)
        file_name = Config.UNTITLED_NAME + '.ipynb'
        _new_path = os.path.join(_path, file_name)
        while exists(_new_path):
            file_name = get_new_untitled_name(file_name)
            _new_path = os.path.join(_path, file_name)
        new_path = os.path.join(parent_path, file_name)
        if 4 * 1024 * 1024 > get_free_space_mb(self.ROOT_PATH):
            raise JptFileSystemError('剩余空间不足')
        # 复制模板过去
        shutil.copyfile(IPYNB_PATH, _new_path)
        chmod777(_new_path)
        return self.file_contents(new_path, not_get_content=True)

    def add_folder(self, parent_path):
        """
        新增 默认文件夹
        :param path:父目录
        :return:
        """
        parent_path = path_legal_verification(parent_path)
        _path = self._init_path(parent_path, is_exists=True, isdir=True)
        file_name = Config.UNTITLED_NAME
        _new_path = os.path.join(_path, file_name)
        while exists(_new_path):
            file_name = get_new_untitled_name(file_name)
            _new_path = os.path.join(_path, file_name)
        new_path = os.path.join(parent_path, file_name)
        if 4 * 1024 * 1024 > get_free_space_mb(self.ROOT_PATH):
            raise JptFileSystemError('剩余空间不足')
        try:
            os.mkdir(_new_path)
        except:
            raise JptFileSystemError("创建文件夹失败：{}".format(str(parent_path)))
        chmod777(_new_path)
        fsync_dir(_new_path)
        return self.file_contents(new_path, not_get_content=True)

    def copy(self, path, new_path, paste_type='duplicate'):
        """复制"""
        path = path_legal_verification(path)
        new_path = path_legal_verification(new_path)
        self._check_new_path(new_path)
        if check_child_path(new_path, path):
            raise JptFileSystemError("文件（夹）不能复制到自己里面 path:{} can't move to new_path:{} ".format(path, new_path))
        if paste_type == 'cover':
            if path == new_path:
                return self.file_contents(new_path, not_get_content=True)
        _path = self._init_path(path, is_exists=True)
        _new_path = self._init_path(new_path)
        is_file = os.path.isfile(_path)

        # 是否 code目录
        add_size = get_path_size(_path)

        if add_size + 4 * 1024 * 1024 > get_free_space_mb(self.ROOT_PATH):
            raise JptFileSystemError('剩余空间不足')

        if paste_type == 'duplicate':
            while exists(_new_path):
                _new_path, new_name = self._get_new_duplicate_path(_new_path)  # 新实际路径
                new_path = os.path.join(os.path.dirname(new_path), new_name)  # 新相对路径

        if is_file:
            copyfile(_path, _new_path)
            chmod777(_new_path)
            fsync_dir(os.path.dirname(_new_path))
        else:
            logger.debug(_path)
            logger.debug(_new_path)
            copy_dir(_path, _new_path)
            chmod777(_new_path)
            fsync_dir(_new_path)

        return self.file_contents(new_path, not_get_content=True)

    def move(self, path, new_path, paste_type='duplicate'):
        """
        移动（剪切）
        :param path: 文件 旧路径
        :param new_path: 文件 新路径
        :return:
        """
        path = path_legal_verification(path)
        if path in [Config.CODE_PATH]:
            raise JptFileSystemError('{} 不可删除'.format(path))
        new_path = path_legal_verification(new_path)
        self._check_new_path(new_path)
        if path == new_path:  # 地址一样的话不移动 返回信息
            return self.file_contents(new_path, not_get_content=True)
        if check_child_path(new_path, path):
            raise JptFileSystemError("文件（夹）不能移动到自己里面 path:{} can't move to new_path:{} ".format(path, new_path))
        _path = self._init_path(path, is_exists=True)
        _new_path = self._init_path(new_path)
        _new_dir_path = os.path.dirname(_new_path)

        is_file = os.path.isfile(_path)

        if not exists(_new_dir_path):
            raise JptFileSystemError('移动的目标目录不存在')

        if paste_type == 'duplicate':
            while exists(_new_path):
                _new_path, new_name = self._get_new_duplicate_path(_new_path)  # 新实际路径
                new_path = os.path.join(os.path.dirname(new_path), new_name)  # 新相对路径

        if is_file:
            shutil.move(_path, _new_path)
            fsync_dir(os.path.dirname(_new_path))
        else:
            move_dir(_path, _new_path)
            chmod777(_new_path)
            fsync_dir(_new_path)

        return self.file_contents(new_path, not_get_content=True)

    def rename(self, path: str, new_name: str, type_limit=None):
        """
        文件改名
        :param path:文件路径
        :param new_name:新名字
        :param type_limit:类型限定   file文件  folder文件夹 不传则都可以
        :return:
        """
        self._check_new_file_name(new_name)
        path = path_legal_verification(path)
        dirname = os.path.dirname(path)  # 根路径
        new_path = os.path.join(dirname, new_name)
        new_path = path_legal_verification(new_path)
        if new_path == path:
            return self.file_contents(new_path, not_get_content=True)
        _path = self._init_path(path, is_exists=True)
        if path in [Config.CODE_PATH]:
            if os.path.isdir(_path):  # CODE文件夹不可重命名
                raise JptFileSystemError('{} 文件夹不可重命名'.format(path))

        _new_path = self._init_path(new_path, no_exists=True)
        is_dir = os.path.isdir(_path)
        if type_limit == 'file':
            if is_dir:
                raise JptFileSystemError('是个文件夹 本接口限定重命名文件')
        elif type_limit == 'folder':
            if not is_dir:
                raise JptFileSystemError('不是个文件夹 本接口限定重命名文件夹')
        try:
            os.rename(_path, _new_path)
        except OSError as e:
            if e.errno == 36:
                raise JptFileSystemError('文件名过长')
            else:
                raise e
        chmod777(_new_path)
        if is_dir:
            fsync_dir(_new_path)
        else:
            fsync_dir(os.path.dirname(_new_path))
        # 新相对路径
        dirname = os.path.dirname(path)
        new_path = os.path.join(dirname, new_name)
        return self.file_contents(new_path, not_get_content=True)

    def delete(self, path):
        """
        删除文件
        :param path: 路径
        :return:
        """
        path = path_legal_verification(path)
        _path = self._init_path(path, is_exists=True, isfile=True)
        os.remove(_path)

    def delete_folder(self, path):
        """
        删除文件夹
        :param path:
        :return:
        """
        path = path_legal_verification(path)
        if path in [Config.CODE_PATH]:
            raise JptFileSystemError('{} 文件夹不可删除'.format(path))
        _path = self._init_path(path, is_exists=True, isdir=True)
        shutil.rmtree(_path)

    def file_list(self, path):
        """目录下所有子文件的内容"""
        _path = self._init_path(path, is_exists=True, isdir=True)
        rows = []
        files = os.listdir(_path)
        if len(files) > Config.FILE_LIST_NUM:
            raise JptFileSystemError(code=AppCode.FILE_LIST_MAX)
        for file in files:
            relative_path = os.path.join(path, file)  # 文件相对路径
            sub_data = self.file_contents(relative_path, not_get_content=True)
            rows.append(sub_data)
        return {'rows': rows}

    def file_contents(self, path, not_get_content=False, init_path=True):
        """
        预览文件内容 信息
        :param path:
        :param not_get_content: 不获取内容
        :param init_path: 是否 转换真实路径
        :return:
        """
        if init_path:
            _path = self._init_path(path, is_exists=True)
        else:
            _path = path
        file_name = os.path.basename(_path)  # 文件名、文件夹名
        last_modified = os.path.getmtime(_path)  # 最后更新时间
        created = os.path.getctime(_path)  # 创建时间
        writable = is_writable(_path)  # 是否可写
        #
        _format = None
        content = None
        mimetype = get_mimetype(file_name)
        is_folder = 0
        is_upload = 0

        if os.path.isdir(_path):
            mimetype = ""
            size = None
            is_folder = 1
        else:  # 文件
            _format = get_format(mimetype)
            size = os.path.getsize(_path)
            if check_upload_file(file_name):
                is_upload = 1

            if not not_get_content:  # 展示内容
                content = get_file_content(_path, _format, size)

        data = {
            'content': content,
            'createdTime': int(created * 1000),
            'format': _format,
            'modifiedTime': int(last_modified * 1000),  # 转毫秒级
            'mimeType': mimetype,
            'name': file_name,
            'path': path,
            'size': size,
            'writable': writable,
            'isFolder': is_folder,
            'isUpload': is_upload
        }
        return data

    # def preview_table_file(self, path, page=1, page_size=1000):
    #     _path = self._init_path(path, isfile=True, is_exists=True)
    #     file_name = os.path.basename(_path)  # 文件名、文件夹名
    #     minetype = get_mimetype(file_name)
    #
    #     try:
    #         total = 0
    #         if minetype == MINE_TYPES['csv']:
    #             df = pd.read_csv(_path, skiprows=(page - 1) * page_size, nrows=page_size)
    #             with open(_path, 'r', encoding='utf-8') as f:
    #                 while f.readline():
    #                     total = total + 1
    #         elif minetype in [MINE_TYPES['xlsx'], MINE_TYPES['xls']]:
    #             with open(_path, 'r', encoding='utf-8') as f:
    #                 while f.readline():
    #                     total = total + 1
    #             df = pd.read_excel(_path, skiprows=(page - 1) * page_size, nrows=page_size)
    #         else:
    #             raise JptFileSystemError('目前只支持 csv xls xlsx文件预览')
    #     except UnicodeDecodeError as e:
    #         raise JptFileSystemError('编码错误 文件必须是utf-8编码 :{}'.format(e))
    #
    #     page_count = total // page_size + 1
    #     return df.to_dict('records'), df.columns.tolist(), page, page_count, total

    def preview_all_csv(self, path, max_column=2000, max_row=1000, max_size=1024 * 1024 * 10, sep=','):
        """打包查询"""
        _path = self._init_path(path, isfile=True, is_exists=True)
        file_name = os.path.basename(_path)  # 文件名、文件夹名
        minetype = get_mimetype(file_name)
        if minetype != MINE_TYPES['csv']:
            raise JptFileSystemError('目前只支持 csv 文件预览')
        try:
            df = pd.read_csv(_path, skiprows=0, nrows=1, sep=sep, encoding='utf-8')
        except pd.errors.EmptyDataError:
            raise JptFileSystemError('内容不能为空')
        except UnicodeDecodeError:
            raise JptFileSystemError('必须是utf-8编码')
        except:
            raise JptFileSystemError('这不是一个标准csv. 内容不能为空，且必须是utf-8编码')
        if len(df.columns) > max_column:
            raise JptFileSystemError('目前预览列数最多为: {}列'.format(max_column))

        try:
            total = -1  # 不算表头
            size = 0
            with open(_path, 'r', encoding='utf-8') as f:
                while True:
                    data = f.readline()
                    if data:
                        size += len(data)
                        total = total + 1
                    else:
                        break
                    if total > max_row:
                        break
                    if size > max_size:
                        break
            df = pd.read_csv(_path, skiprows=0, nrows=total, sep=sep)
        except UnicodeDecodeError as e:
            raise JptFileSystemError('编码错误 文件必须是utf-8编码 :{}'.format(e))
        # rows = df_to_dict(df)
        df2 = df.fillna(np.nan).replace([np.nan], [None])
        rows = df2.to_dict('records')
        return rows, df.columns.tolist(), total

    def decompress_file(self, path):
        """解压文件"""
        path = path_legal_verification(path)
        self._check_new_path(path)
        _path = self._init_path(path, is_exists=True, isfile=True)
        parent_path = os.path.dirname(path)
        _parent_path = os.path.dirname(_path)

        # 文件类型校验 和 替换
        file_name = os.path.basename(_path)  # 文件名、文件夹名
        decompress_types = ['.tar.gz', '.zip', '.tgz', ".tbz", ".tbz2", ".tar.bz", ".tar.bz2", ".txz", ".tar.xz"]
        new_name = file_name
        for _type in decompress_types:
            if new_name.endswith(_type):
                new_name = new_name[:len(new_name) - len(_type)]
                break
        if new_name == file_name:
            raise JptFileSystemError('不支持该文件解压')
        # 生成 解压路径
        _new_path = os.path.join(_parent_path, new_name)
        while exists(_new_path):
            _new_path, new_name = self._get_new_duplicate_path(_new_path)  # 新实际路径
        new_path = os.path.join(parent_path, new_name)  # 新相对路径
        # 解压
        try:
            decompress(_path, _new_path)
        except Exception as e:  # 汉化
            if 'File is not a zip file' in str(e):
                raise JptFileSystemError('当前文件不是zip文件')
            else:
                logger.error(str(e))
                raise JptFileSystemError('当前文件不是zip文件')
        if exists(_new_path):
            chmod777(_new_path)
        else:
            raise JptFileSystemError('此zip文件为空')
        return self.file_contents(new_path)

    def update(self, path: str, _format: str, contents):
        """
        write
        :param path: 文件
        :param contents: 内容
        :return:
        """
        path = path_legal_verification(path)
        try:
            _path = self._init_path(path, is_exists=True, isfile=True)
            if _format == 'text':
                if not isinstance(contents, str):
                    raise JptFileSystemError('参数contents 不是个 字符串')
                with open(_path, 'w') as f:
                    try:
                        portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)  # 非阻塞排他锁
                    except portalocker.exceptions.LockException:
                        raise JptFileSystemError(code=AppCode.File_Open_Fail)
                    f.write(contents)
                    f.flush()
                    os.fsync(f.fileno())
            elif _format == 'json':
                if not isinstance(contents, dict):
                    raise JptFileSystemError('参数contents 不是个 json')
                contents = json.dumps(contents, ensure_ascii=False, indent=1)
                with open(_path, 'w') as f:
                    try:
                        portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)  # 非阻塞排他锁
                    except portalocker.exceptions.LockException:
                        raise JptFileSystemError(code=AppCode.File_Open_Fail)
                    f.write(contents)
                    f.flush()
                    os.fsync(f.fileno())
            else:
                raise Exception('必须传入 _format')
            chmod777(_path)
            return self.file_contents(path, not_get_content=True)
        except Exception as e:
            raise e


if __name__ == '__main__':
    from pprint import pprint
    import redis

    redis_store = redis.StrictRedis(host='10.76.69.7', port='36379',
                                    password='Yunanbao2016_redis@yab#1', db=7, decode_responses=True)
    controller = FileObjController('/home/xiang/文档/测试文件/', rds=redis_store)
    # controller.decompress_file('/ok.zip')
    controller.decompress_file('/测试.zip')
    controller.decompress_file('/111.zip')
    # controller.decompress_file('/jia.zip')
    # controller.decompress_file('/1.zip')
    # controller.copy('/资源 68-副本1.png', '/资源 68-副本1.png')
    # controller.copy('/"资源 68-副本1.png', '/"资源 68-副本1.png')
    # print(controller.decompress_file('/zip1.zip'))
    # controller._check_new_path('.abd')
    # print(controller.preview_all_csv('/tests.csv'))
    # print(controller.preview_all_csv('/test2.csv'))
    # print(controller.preview_all_csv('/aRowACol.csv'))
    # pprint(controller.file_contents('/aRowACol.csv'))
    # controller._check_git_dir('/CODE/.git')
    # controller._check_git_dir('/CODE/.git/2  ')

    # check = controller.check_exists_in_folder('tests', '/')
    # print(check)
    # print(controller._init_path('//tests'))
    # print(controller._init_path('/tests'))

    # rows, columns, page, page_count, total = controller.preview_table_file('/tests.xls', 1, 2)
    # pprint(rows)
    # pprint(columns)
    # print(page, page_count, total)
    # pprint(controller.file_list("/"))
    # print(controller.utime("/tests.txt", time.time()))
    # pprint(controller.contents("/home/xiang/workproject/jpt_filesystem/tests/tests.txt"))
    # pprint(controller.contents("/home/xiang/workproject/jpt_filesystem/tests/tests.csv"))
    # pprint(controller.contents("/home/xiang/workproject/jpt_filesystem/tests/tests.ipynb"))
    # pprint(controller.contents("/home/xiang/workproject/jpt_filesystem/tests"))
    # f = open("/home/xiang/workproject/jpt_filesystem/tests/text.json", 'r')
    # contents_json = f.read()
    # f.close()
    # contents = json.loads(contents_json)
    # print(type(contents))
    # print(contents)
    # pprint(
    #     controller.update("/home/xiang/workproject/jpt_filesystem/tests/xxx2.ipynb", _format='json', contents=contents))
    # controller.copy('/home/xiang/workproject/jpt_filesystem/tests/tests.txt')
    # controller.zip_dir('/log')
    # print(controller.update("/home/xiang/workproject/jpt_filesystem/tests/tests.txt", '66666666'))

    # controller.rename('/home/xiang/workproject/jpt_filesystem/tests/testdir2', 'testdir2')

    # controller.add('/')
    # controller.delete('/untitled')
    # controller.add_folder('/')
    # controller.delete_folder('/untitled')
    # controller.add_ipynb('/')
    # controller.delete('/Untitled.ipynb')
    # controller.copy('/nb', '/nb/nb', is_file=False)
    # controller.move('/nb/123.txt', '/nb/nb/123.txt')
