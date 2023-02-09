"""
run_in_threadpool(method,*args,**kwargs)
"""
import base64
import os
import shutil
import json
import anyio
from starlette.concurrency import run_in_threadpool

from fps_file_server.common.file_tools import get_file_content, chmod777, chmod771, copyfile, copy_dir, move_dir, \
    fsync_dir, nfs_flush, get_path_size, zip_dir
from fps_file_server.config import Config


class AioFileTool:
    async def fsync_dir(self, path):
        if await anyio.Path(path).is_dir():
            await run_in_threadpool(fsync_dir, path)

    async def nfs_flush(self, path):
        await run_in_threadpool(nfs_flush, path)

    async def get_path_size(self, path):
        return await run_in_threadpool(get_path_size, path)

    async def copy_file(self, path, new_path):
        await run_in_threadpool(copyfile, path, new_path)
        parent_path = anyio.Path(new_path).parent
        await self.fsync_dir(parent_path)

    async def copy_dir(self, path, new_path):
        await run_in_threadpool(copy_dir, path, new_path)
        await self.fsync_dir(new_path)

    async def move_file(self, path, new_path):
        await run_in_threadpool(shutil.move, path, new_path)
        parent_path = os.path.dirname(new_path)
        await self.fsync_dir(parent_path)

    async def move_dir(self, path, new_path):
        await run_in_threadpool(move_dir, path, new_path)
        await self.fsync_dir(new_path)

    async def delete(self, path):
        await run_in_threadpool(os.remove, path)

    async def delete_dir(self, path):
        await run_in_threadpool(shutil.rmtree, path)

    async def get_file_content(self, path, _format: str, size=None):
        if size is None:
            size = await self.get_path_size(path)
        content = None
        if _format.lower() == 'json':
            try:
                content = await anyio.Path(path).read_text(encoding='utf-8')
            except UnicodeDecodeError:
                raise Exception('只支持预览utf-8编码文件')

            content = json.loads(content)  # json转dict 后面view层return会转json

        elif _format.lower() == 'images':
            if size < Config.IMAGES_PREVIEW_SIZE:
                content_bytes = await anyio.Path(path).read_bytes()
                content = base64.b64encode(content_bytes)
        elif _format.lower() == 'text':
            if size < Config.TEXT_PREVIEW_SIZE:
                content = await anyio.Path(path).read_text(encoding='utf-8')
        return content

    async def chmod777(self, path):
        await run_in_threadpool(chmod777, path)
        await self.fsync_dir(path)

    async def chmod771(self, path):
        await run_in_threadpool(chmod771, path)
        await self.fsync_dir(path)

    async def zip_dir(self, dir_path, out_path):
        await run_in_threadpool(zip_dir, dir_path, out_path)

    async def read_file(self, path, encoding='utf-8'):
        return await anyio.Path(path).read_text(encoding=encoding)

    async def write_file(self, path, data=None, encoding='utf-8'):
        # await anyio.Path(path).write_text(data, encoding=encoding)
        async with await anyio.open_file(path, 'w') as f:
            if data is not None:
                await f.write(data)
            await f.flush()
            await run_in_threadpool(os.fsync, f._fp.fileno())
    async def mkdir(self, path):
        await anyio.Path(path).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    import anyio

    aio_file_tool = AioFileTool()
    # content = anyio.run(aio_file_tool.get_file_content, './file_obj_controller.py', 'text')
    # print(content)
    # print(anyio.run(aio_file_tool.get_path_size, './__init__.py'))
    # print(anyio.run(aio_file_tool.get_path_size, './file_obj_controller.py'))
    # print(anyio.run(aio_file_tool.get_path_size, './file_obj_controller.py'))
    # print(anyio.run(aio_file_tool.chmod777, './file_obj_controller.py'))
    # print(anyio.run(aio_file_tool.copy_file, './file_obj_controller.py', './xxx.py'))
    # print(anyio.run(aio_file_tool.copy_dir, './', '../test'))
    # print(anyio.run(aio_file_tool.delete, './xxx.py'))
    # print(anyio.run(aio_file_tool.delete_dir, './xxx'))
    # print(anyio.run(aio_file_tool.nfs_flush, './file_obj_controller.py'))
    # print(anyio.run(aio_file_tool.nfs_flush, './xxx'))
    # print(anyio.run(aio_file_tool.move_file, './2.txt', './3.txt'))
    # print(anyio.run(aio_file_tool.move_dir, './xxx', './yyy'))
    # print(anyio.run(aio_file_tool.get_file_content, './file_obj_controller.py', 'json'))
    # print(anyio.run(aio_file_tool.mkdir, './xxz/xx'))
    # print(anyio.run(aio_file_tool.write_file, './fps.log', 'xxx'))
