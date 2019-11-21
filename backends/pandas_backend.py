from pandas import DataFrame
from pandas import read_pickle as pd_load

from mldb.backends.validation import validate_dtype
from mldb.backends.filesystem_backend import FileSystemInterface, FileSystemBase

__all__ = [
    'PandasBackend', 'PandasInterface'
]


class PandasInterface(FileSystemInterface):
    def load(self):
        data = pd_load(path=self.path, compression='gzip')
        validate_dtype(data, DataFrame)
        return data
    
    def save(self, data):
        validate_dtype(data, DataFrame)
        data.to_pickle(path=self.path, compression='gzip')


class PandasBackend(FileSystemBase):
    def __init__(self, path):
        super(PandasBackend, self).__init__(interface=PandasInterface, ext='pd', path=path)
