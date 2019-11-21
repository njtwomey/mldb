import json

from mldb.backends.filesystem_backend import FileSystemBase, FileSystemInterface

__all__ = [
    'JsonInterface', 'JsonBackend',
]


class JsonInterface(FileSystemInterface):
    def __init__(self, name, **kwargs):
        super(JsonInterface, self).__init__(path=name)
        self.json_kwargs = dict(kwargs)
    
    def load(self):
        with open(self.path, 'r') as fil:
            return json.load(fil)
    
    def save(self, data):
        with open(self.path, 'w') as fil:
            return json.dump(data, fil, **self.json_kwargs)


class JsonBackend(FileSystemBase):
    def __init__(self, path, sort_keys=True, indent=None, cls=None):
        super(JsonBackend, self).__init__(path=path, ext='json', interface=JsonInterface)
        self.json_kwargs = dict(sort_keys=sort_keys, indent=indent, cls=cls)
    
    def get(self, name, *args, **kwargs):
        return super(JsonBackend, self).get(name, **self.json_kwargs)
