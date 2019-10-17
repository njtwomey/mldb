import joblib

from mldb.backends.filesystem_backend import FileSystemInterface, FileSystemBase

__all__ = [
    'JoblibBackend', 'JoblibInterface'
]


class JoblibInterface(FileSystemInterface):
    def load(self):
        return joblib.load(self.path)
    
    def save(self, data):
        joblib.dump(data, self.path)


class JoblibBackend(FileSystemBase):
    def __init__(self, path):
        super(JoblibBackend, self).__init__(
            interface=JoblibInterface,
            ext='joblib',
            path=path,
        )
