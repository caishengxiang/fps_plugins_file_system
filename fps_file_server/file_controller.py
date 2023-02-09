import os
import pathlib
import time
import anyio
import pandas as pd
import numpy as np
from starlette.concurrency import run_in_threadpool
from fps_file_server.common.aio_file_tools import AioFileTool
from fps_file_server.exceptions import FileServerError as Error
from fps_file_server.common.file_tools import root_path_change, exists, check_file_name_length_available, \
    path_legal_verification, is_writable, get_mimetype, get_format, check_upload_file, get_new_untitled_name, \
    get_free_space_mb, get_path_size
from fps_file_server.common.utils import get_new_file_name
from fps_file_server.config import Config
from fps_file_server.common.mine_types import MINE_TYPES
from fps_file_server.common.notebook_template import UNTITLED_NOTEBOOK


class FileObjController:
    """文件操作控制器"""

    def __init__(self, root_path, rds=None, experiment_id=None):
        self.ROOT_PATH = root_path
        self.rds = rds
        self.experiment_id = experiment_id
        self.aio_tool = AioFileTool()

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
            raise Error("文件名称不合法,不能以'/'或'.' 开头")
        if file_name.startswith('.'):
            raise Error("文件名称不合法,不能以'/'或'.' 开头")
        if not check_file_name_length_available(file_name):
            raise Error('文件名称最长64个字符')  # 响应信息是产品要求

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
            raise Error('文件名称最长64个字符')
        return new_path, new_name

    async def get_path_size(self, path):
        _path = self._init_path(path)
        await run_in_threadpool(get_path_size, _path)

    async def create_folder(self, path, paste_type='duplicate'):
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
        await self.aio_tool.chmod777(_new_path)
        info = await self.file_contents(new_path, get_content=False)
        return info

    async def file_contents(self, path, get_content=True, init_path=True):
        """
        预览文件内容 信息
        :param path:
        :param get_content: 获取内容
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
            size = await self.aio_tool.get_path_size(_path)
            if check_upload_file(file_name):
                is_upload = 1

            if get_content:  # 展示内容
                content = await self.aio_tool.get_file_content(_path, _format, size)

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

    async def add(self, parent_path):
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
            raise Error('剩余空间不足')
        await self.aio_tool.write_file(_new_path)
        await self.aio_tool.chmod777(_new_path)
        return await self.file_contents(new_path, get_content=False)

    async def add_notebook(self, parent_path):
        """
        新建 空notebook
        :param parent_path: 目录
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
            raise Error('剩余空间不足')
        await self.aio_tool.write_file(_new_path, UNTITLED_NOTEBOOK)
        await self.aio_tool.chmod777(_new_path)
        return await self.file_contents(new_path, get_content=False)

    async def add_folder(self, parent_path):
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
            raise Error('剩余空间不足')
        try:
            await self.aio_tool.mkdir(_new_path)
        except:
            raise Error("创建文件夹失败：{}".format(str(parent_path)))

        await self.aio_tool.chmod777(_new_path)
        return await self.file_contents(new_path, get_content=False)

    def _preview_all_csv(self, path, max_column=2000, max_row=1000, max_size=1024 * 1024 * 10, sep=','):
        """打包查询"""
        _path = self._init_path(path, isfile=True, is_exists=True)
        file_name = os.path.basename(_path)  # 文件名、文件夹名
        minetype = get_mimetype(file_name)
        if minetype != MINE_TYPES['csv']:
            raise Error('目前只支持 csv 文件预览')
        try:
            df = pd.read_csv(_path, skiprows=0, nrows=1, sep=sep, encoding='utf-8')
        except pd.errors.EmptyDataError:
            raise Error('内容不能为空')
        except UnicodeDecodeError:
            raise Error('必须是utf-8编码')
        except:
            raise Error('这不是一个标准csv. 内容不能为空，且必须是utf-8编码')
        if len(df.columns) > max_column:
            raise Error('目前预览列数最多为: {}列'.format(max_column))

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
            raise Error('编码错误 文件必须是utf-8编码 :{}'.format(e))
        df2 = df.fillna(np.nan).replace([np.nan], [None])
        rows = df2.to_dict('records')
        return rows, df.columns.tolist(), total

    async def preview_all_csv(self, path, max_column=2000, max_row=1000, max_size=1024 * 1024 * 10, sep=','):
        return await run_in_threadpool(self._preview_all_csv, path, max_column=max_column, max_row=max_row,
                                       max_size=max_size,
                                       sep=sep)


if __name__ == '__main__':
    import anyio

    controller = FileObjController('/home/xiang/workproject/fps_plugins_file_system/fps_file_server')
    # a, b, c = anyio.run(controller.preview_all_csv, '/x.csv')
    # print(a, b, c)
