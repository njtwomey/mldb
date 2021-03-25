import yaml

from mldb.backends.filesystem_backend import FileSystemBase
from mldb.backends.filesystem_backend import FileSystemInterface

__all__ = [
    "YamlInterface",
    "YamlBackend",
]


class YamlInterface(FileSystemInterface):
    def __init__(self, name):
        super(YamlInterface, self).__init__(path=name)

    def load(self):
        with open(self.path, "r") as fil:
            return yaml.safe_load(fil)

    def save(self, data):
        with open(self.path, "w") as fil:
            yaml.safe_dump(data, fil)


class YamlBackend(FileSystemBase):
    def __init__(self, path):
        super(YamlBackend, self).__init__(path=path, ext="yaml", interface=YamlInterface)
