from numpy import load as np_load
from numpy import save as np_save
from numpy import ndarray

from mldb.backends.validation import validate_dtype
from mldb.backends.filesystem_backend import FileSystemInterface, FileSystemBase

__all__ = [
    'NumpyInterface', 'NumpyBackend'
]


class NumpyInterface(FileSystemInterface):
    def load(self):
        data = np_load(file=self.path, allow_pickle=True)
        validate_dtype(data, ndarray)
        return data
    
    def save(self, data):
        validate_dtype(data, ndarray)
        np_save(file=self.path, arr=data, allow_pickle=True)


class NumpyBackend(FileSystemBase):
    def __init__(self, path):
        super(NumpyBackend, self).__init__(
            interface=NumpyInterface,
            ext='npy',
            path=path,
        )
