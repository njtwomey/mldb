from os.path import exists, join
from os import makedirs, unlink, sep

from mldb.backends.base import BackendInterface, Backend

__all__ = [
    'FileSystemInterface', 'FileSystemBase',
]


class FileSystemInterface(BackendInterface):
    def __init__(self, path):
        super(FileSystemInterface, self).__init__(path=path)
        
        # Ensure that the output directory path exists
        path_split = path.split(sep)
        if len(path_split) > 1:
            path_join = sep.join(path_split[:-1])
            makedirs(path_join, exist_ok=True)
    
    def exists(self):
        return exists(self.path)
    
    def delete(self):
        return unlink(self.path)


class FileSystemBase(Backend):
    def __init__(self, interface, path, ext):
        super(FileSystemBase, self).__init__(interface=interface)
        
        self.ext = ext
        self.path = path
        
        makedirs(path, exist_ok=True)
    
    def get(self, name, *args, **kwargs):
        path = join(self.path, f'{name}.{self.ext}')
        return self.interface(
            path, *args, **kwargs
        )
