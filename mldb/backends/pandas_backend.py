from pandas import DataFrame
from pandas import read_pickle as pd_load
from pandas import Series

from mldb.backends.filesystem_backend import FileSystemBase
from mldb.backends.filesystem_backend import FileSystemInterface
from mldb.backends.validation import validate_dtype

__all__ = ["PandasBackend", "PandasInterface"]


PANDAS_DTYPES = (Series, DataFrame)


class PandasInterface(FileSystemInterface):
    def load(self):
        data = pd_load(filepath_or_buffer=self.path, compression="gzip")
        validate_dtype(data, PANDAS_DTYPES)
        return data

    def save(self, data):
        validate_dtype(data, PANDAS_DTYPES)
        data.to_pickle(path=self.path, compression="gzip")


class PandasBackend(FileSystemBase):
    def __init__(self, path):
        super(PandasBackend, self).__init__(interface=PandasInterface, ext="pd", path=path)
