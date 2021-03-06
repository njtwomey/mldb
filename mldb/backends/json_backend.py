import json
from pprint import pformat

import numpy as np

from mldb.backends.filesystem_backend import FileSystemBase
from mldb.backends.filesystem_backend import FileSystemInterface

__all__ = [
    "JsonInterface",
    "JsonBackend",
]


# Solve errors in serialising numpy types to JSON, via:
# https://stackoverflow.com/questions/50916422/python-typeerror-object-of-type-int64-is-not-json-serializable
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def prettify_json(data, json_kwargs):
    d = pformat(json.loads(json.dumps(data, **json_kwargs)), indent=1, width=120)
    return (
        d.replace('"', "__double_quote__")
        .replace("'", "__single_quote__")
        .replace("__single_quote__", '"')
        .replace("__double_quote__", "'")
    )


class JsonInterface(FileSystemInterface):
    def __init__(self, name, pprint, **kwargs):
        super(JsonInterface, self).__init__(path=name)
        self.pprint = pprint
        self.json_kwargs = dict(kwargs)

    def load(self):
        with open(self.path, "r") as fil:
            return json.load(fil)

    def save(self, data):
        with open(self.path, "w") as fil:
            if self.pprint:
                fil.write(prettify_json(data, self.json_kwargs))
            else:
                json.dump(data, fil, **self.json_kwargs, cls=NpEncoder)


class JsonBackend(FileSystemBase):
    def __init__(self, path, sort_keys=True, indent=None, cls=None, pprint=True):
        super(JsonBackend, self).__init__(path=path, ext="json", interface=JsonInterface)
        self.json_kwargs = dict(sort_keys=sort_keys, indent=indent, cls=cls)

    def get(self, name, *args, **kwargs):
        return super(JsonBackend, self).get(name, pprint=True, **self.json_kwargs)
