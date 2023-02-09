# -*-coding:utf-8-*-
from pydantic import BaseModel, Field
from enum import Enum


class CheckSameParam(BaseModel):
    parent_path: str = Field(..., description='相对父路径')
    name: str = Field(..., description='文件（夹）名')

class FileListParam(BaseModel):
    parent_path: str = Field(..., description='相对父路径')


class AddParam(BaseModel):
    parent_path: str = Field(..., description='相对父路径')


class DeleteParam(BaseModel):
    path: str = Field(..., description='相对路径')


class MoveParam(BaseModel):
    path: str = Field(..., description='相对路径')
    target_parent_path: str = Field(..., description='目标相对父路径')
    name: str = Field(..., description='文件（夹）名')
    duplicate: bool = Field(False, description='是否副本')


class UnzipParam(BaseModel):
    path: str = Field(..., description='相对路径')


class FolderSizeParam(BaseModel):
    path: str = Field(..., description='相对路径')


class ContentsParam(BaseModel):
    path: str = Field(..., description='相对路径')
    content: bool = Field(..., description='是否展示内容')


class RenameParam(BaseModel):
    path: str = Field(..., description='相对路径')
    new_name: str = Field(..., description='新文件（夹）名')


class CreateFolderParam(BaseModel):
    path: str = Field(..., description='相对路径')


class CreateFoldersParam(BaseModel):
    paths: list = Field(..., description='相对路径列表')


class PreviewCsvParam(BaseModel):
    path: str = Field(..., description='相对路径')
    sep: str = Field(..., description='分隔符')
    max_column: int = Field(2000, description='最大展示列数')
    max_row: int = Field(2000, description='最大展示行数')
    max_size: int = Field(10 * 1024 * 1024, description='最大展示字节数')


class PreviewImageParam(BaseModel):
    path: str = Field(..., description='相对路径')


class FormatEnum(str, Enum):
    text = 'text'
    json = 'json'


class UpdateParam(BaseModel):
    path: str = Field(..., description='相对路径')
    format: str = Field(FormatEnum, description='内容格式 text/json')
    content: [str, dict] = Field(..., description='文件内容')
