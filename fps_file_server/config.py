from fps.config import PluginModel
from fps.config import get_config as fps_get_config
from fps.hooks import register_config, register_plugin_name


class FileServerConfig(PluginModel):
    IMAGES_PREVIEW_SIZE: int = 20 * 1024 * 1024
    TEXT_PREVIEW_SIZE: int = 10 * 1024 * 1024
    UPLOAD_FILE_EXTENSION: str = 'temp_upload'
    UNTITLED_NAME: str = 'Untitled'
    UNTITLED_COMPILE: str = r'^(Untitled?)(\d+?)$'

Config = FileServerConfig()

def get_config():
    return fps_get_config(FileServerConfig)


c = register_config(FileServerConfig)
n = register_plugin_name("FileServer")
