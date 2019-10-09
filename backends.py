from os.path import exists, join
from os import makedirs, remove, sep

import json
import pickle

from mldb.logger import get_logger

logger = get_logger(__name__)

__all__ = [
    'Backend',
    'VolatileBackend',
    'FileSystemBase',
    'PickleBackend',
    'JsonBackend',
]


class Backend(object):
    def __init__(self):
        """
        
        """
        
        self.cache_data = True
    
    def exists(self, name):
        """
        
        :param name:
        
        :return:
        """
        
        raise NotImplementedError
    
    def load_data(self, name):
        """
        
        :param name:
        
        :return:
        """
        
        raise NotImplementedError
    
    def save_data(self, name, data):
        """
        
        :param name:
        
        :param data:
        :return:
        """
        
        raise NotImplementedError
    
    def del_node(self, name):
        """
        
        :param name:
        
        :return:
        """
        
        raise NotImplementedError
    
    def prepare(self, name):
        """
        
        :param name:
        :return:
        """
        pass


"""
VOLATILE BACKEND

A non-persistent backend
"""


class VolatileBackend(Backend):
    def exists(self, name):
        return False
    
    def load_data(self, name):
        raise NotImplementedError
    
    def save_data(self, name, data):
        pass


"""
FILESYSTEM BACKENDS
"""


class FileSystemBase(Backend):
    def __init__(self, path, ext):
        """
        
        :param path:
        :param ext:
        """
        
        super(FileSystemBase, self).__init__()
        
        self.ext = ext
        self.path = path
        
        if not exists(self.path):
            makedirs(path)
    
    def create_path(self, path):
        path_split = path.split(sep)
        if len(path_split) > 1:
            path_join = sep.join(path_split[:-1])
            if not exists(path_join):
                makedirs(path_join)
    
    def node_path(self, name):
        """
        
        :param name:
        :return:
        """
        
        return join(self.path, '{}.{}'.format(
            name,
            self.ext
        ))
    
    def exists(self, name):
        """
        
        :param name:
        :return:
        """
        
        ex = exists(self.node_path(name=name))
        return ex
    
    def load_data(self, name):
        """
        
        :param name:
        :return:
        """
        
        raise NotImplementedError
    
    def save_data(self, name, data):
        """
        
        :param name:
        :param data:
        :return:
        """
        
        raise NotImplementedError
    
    def del_node(self, name):
        """
        
        :param name:
        :return:
        """
        
        remove(self.node_path(name))
    
    def prepare(self, name):
        """
        Called before the function and save_data.
        
        :param name:
        :return:
        """
        node_path = self.node_path(name=name)
        self.create_path(node_path)


class JsonBackend(FileSystemBase):
    def __init__(self, path, sort_keys=True, indent=None, cls=None):
        """
        
        :param path:
        """
        
        super(JsonBackend, self).__init__(path, 'json')
        self.sort_keys = sort_keys
        self.indent = indent
        self.cls = cls
    
    def load_data(self, name):
        """
        
        :param name:
        :return:
        """
        
        return json.load(open(self.node_path(name=name), 'r'))
    
    def save_data(self, name, data):
        """
        
        :param name:
        :param data:
        :return:
        """
        
        json.dump(
            data,
            open(self.node_path(name=name), 'w'),
            sort_keys=self.sort_keys,
            indent=self.indent,
            cls=self.cls,
        )


class PickleBackend(FileSystemBase):
    def __init__(self, path):
        """

        :param path:
        """
        
        super(PickleBackend, self).__init__(path, 'pkl')
    
    def load_data(self, name):
        """

        :param name:
        :return:
        """
        
        return pickle.load(open(self.node_path(name=name), 'rb'))
    
    def save_data(self, name, data):
        """

        :param name:
        :param data:
        :return:
        """
        
        node_path = self.node_path(name=name)
        pickle.dump(data, open(node_path, 'wb'))
