from pathlib import Path

from mldb.backends.base import BackendInterface, Backend

__all__ = [
    "FileSystemInterface",
    "FileSystemBase",
]


class FileSystemInterface(BackendInterface):
    def __init__(self, path):
        super(FileSystemInterface, self).__init__(path=path)

        # Ensure that the output directory path exists
        self.prepare()

    def exists(self):
        return self.path.exists()

    def delete(self):
        return self.path.unlink()


class FileSystemBase(Backend):
    def __init__(self, interface, path, ext):
        super(FileSystemBase, self).__init__(interface=interface)

        self.ext = ext
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def get(self, name, *args, **kwargs):
        path = self.path / f"{name}.{self.ext}"
        return self.interface(path, *args, **kwargs)
