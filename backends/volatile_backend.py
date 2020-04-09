from mldb.backends.base import BackendInterface, Backend

__all__ = ["VolatileBackend", "VolatileInterface"]


class VolatileInterface(BackendInterface):
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
        super(VolatileBackend, self).__init__(interface=VolatileInterface)
