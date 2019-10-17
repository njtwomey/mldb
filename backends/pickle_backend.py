import pickle

from mldb.backends.filesystem_backend import FileSystemInterface, FileSystemBase

__all__ = [
    'PickleBackend', 'PickleInterface',
]


class PickleInterface(FileSystemInterface):
    def load(self):
        with open(self.path, 'rb') as fil:
            return pickle.load(fil)
    
    def save(self, data):
        with open(self.path, 'wb') as fil:
            pickle.dump(data, fil)


class PickleBackend(FileSystemBase):
    def __init__(self, path):
        super(PickleBackend, self).__init__(
            interface=PickleInterface,
            path=path,
            ext='pkl',
        )
