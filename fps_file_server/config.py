from fps.config import PluginModel
from fps.config import get_config as fps_get_config
from fps.hooks import register_config, register_plugin_name


class FileServerConfig(PluginModel):
    random: bool = False
    greeting: str = "FileServer"
    count: int = 0


def get_config():
    return fps_get_config(FileServerConfig)


c = register_config(FileServerConfig)
n = register_plugin_name("FileServer")
