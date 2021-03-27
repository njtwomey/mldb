from mldb.backends.base import Backend
from mldb.backends.base import BackendInterface

__all__ = ["VolatileBackend", "VolatileInterface"]


class VolatileInterface(BackendInterface):
    def __init__(self, name):
        super().__init__(path=f"/tmp/{name}")

    def exists(self):
        return False

    def load(self):
        raise NotImplementedError

    def save(self, data):
        pass

    def delete(self):
        pass


class VolatileBackend(Backend):
    def __init__(self):
        super(VolatileBackend, self).__init__(interface=VolatileInterface, cache_data=False)
