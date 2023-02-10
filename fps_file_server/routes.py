# -*-coding:utf-8-*-
import random

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fps_file_server.config import Config
from fps_file_server.exceptions import RedirectException

from fps.exceptions import RedirectException as FpsRedirectException
from fps.hooks import register_router
from fps_file_server.params import *
from fps_file_server.file_controller import FileObjController


def render(msg='ok', code=0, data=None, status_code=200):
    return JSONResponse({'code': code, 'msg': msg, 'data': data}, status_code=status_code)


r = APIRouter()


@r.api_route("/folder-size", methods=['GET', "POST"])
async def folder_size(param: FolderSizeParam):
    controller = FileObjController(Config.root_path)
    size = await controller.get_path_size(param.path)
    return render(data={'folder_size': size})


@r.api_route("/contents", methods=['GET', "POST"])
async def contents(param: ContentsParam):
    controller = FileObjController(Config.root_path)
    content = await controller.file_contents(param.path, get_content=param.content)
    raise render(data={'contents': content})


@r.api_route("/add", methods=['GET', "POST"])
async def add(param: AddParam):
    controller = FileObjController(Config.root_path)
    file_info = await controller.add(param.parent_path)
    raise render(data=file_info)


@r.api_route("/add-notebook", methods=['GET', "POST"])
async def add_notebook(param: AddParam):
    controller = FileObjController(Config.root_path)
    file_info = await controller.add(param.parent_path)
    return render(data={})


@r.api_route("/add-folder", methods=['GET', "POST"])
async def add_folder(param: AddParam):
    return render(data={})


@r.api_route("/delete", methods=['GET', "POST"])
async def delete(param: DeleteParam):
    return render(data={})


@r.api_route("/delete-folder", methods=['GET', "POST"])
async def delete_folder(param: DeleteParam):
    return render(data={})


@r.api_route("/move", methods=['GET', "POST"])
async def move(param: MoveParam):
    return render(data={})


@r.api_route("/unzip", methods=['GET', "POST"])
async def unzip(param: UnzipParam):
    return render(data={})


@r.api_route("/rename", methods=['GET', "POST"])
async def rename(param: RenameParam):
    return render(data={})


@r.api_route("/rename-folder", methods=['GET', "POST"])
async def rename_folder(param: RenameParam):
    return render(data={})


@r.api_route("/create-folder", methods=['GET', "POST"])
async def create_folder(param: CreateFolderParam):
    return render(data={})


@r.api_route("/create-folders", methods=['GET', "POST"])
async def create_folders(param: CreateFoldersParam):
    return render(data={})


@r.api_route("/preview-csv", methods=['GET', "POST"])
async def preview_csv(param: PreviewCsvParam):
    return render(data={})


@r.api_route("/preview-image", methods=['GET', "POST"])
async def preview_image(param: PreviewImageParam):
    return render(data={})


@r.api_route("/update", methods=['GET', "POST"])
async def preview_image(param: PreviewImageParam):
    return render(data={})


@r.api_route("/list", methods=['GET', "POST"])
async def file_list(param: FileListParam):
    raise render(data={'contents'})


router = register_router(r)
