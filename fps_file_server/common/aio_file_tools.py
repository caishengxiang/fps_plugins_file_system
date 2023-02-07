"""
run_in_threadpool(method,*args,**kwargs)
"""
import os
import shutil

from starlette.concurrency import run_in_threadpool

from fps_file_server.common.file_tools import get_file_content, chmod777, chmod771, copyfile, copy_dir, move_dir, \
    fsync_dir, nfs_flush, get_path_size, zip_dir
from fps_file_server.config import Config


class AioFileTool:
    async def fsync_dir(self, path):
        await run_in_threadpool(fsync_dir, path)

    async def nfs_flush(self, path):
        await run_in_threadpool(nfs_flush, path)

    async def get_path_size(self, path):
        return await run_in_threadpool(get_path_size, path)

    async def copy_file(self, path, new_path):
        await run_in_threadpool(copyfile, path, new_path)
        await self.fsync_dir(new_path)

    async def copy_dir(self, path, new_path):
        await run_in_threadpool(copy_dir, path, new_path)
        await self.fsync_dir(new_path)

    async def move_file(self, path, new_path):
        await run_in_threadpool(shutil.move, path, new_path)
        await self.fsync_dir(new_path)

    async def move_dir(self, path, new_path):
        await run_in_threadpool(move_dir, path, new_path)
        await self.fsync_dir(new_path)

    async def delete(self, path):
        await run_in_threadpool(os.remove, path)

    async def delete_dir(self, path):
        await run_in_threadpool(shutil.rmtree, path)

    async def get_file_content(self, path, _format, size=None):
        return await run_in_threadpool(get_file_content, path, _format, size)

    async def chmod777(self, path):
        await run_in_threadpool(chmod777, path)

    async def chmod771(self, path):
        await run_in_threadpool(chmod771, path)

    async def zip_dir(self, dir_path, out_path):
        await run_in_threadpool(zip_dir, dir_path, out_path)


if __name__ == '__main__':
    import anyio

    aio_file_tool = AioFileTool()
    content = anyio.run(aio_file_tool.get_file_content, './file_obj_controller.py', 'text')
    print(content)
    print(anyio.run(aio_file_tool.get_path_size, './__init__.py'))
    print(anyio.run(aio_file_tool.get_path_size, './file_obj_controller.py'))
