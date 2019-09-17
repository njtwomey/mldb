from os.path import exists, join
from os import makedirs, remove, sep

import json
import pickle

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
        
        cache_data = True
    
    def exists(self, node_name):
        """
        
        :param node_name:
        
        :return:
        """
        
        raise NotImplementedError
    
    def load_data(self, node_name):
        """
        
        :param node_name:
        
        :return:
        """
        
        raise NotImplementedError
    
    def save_data(self, node_name, data):
        """
        
        :param node_name:
        
        :param data:
        :return:
        """
        
        raise NotImplementedError
    
    def del_node(self, node_name):
        """
        
        :param node_name:
        
        :return:
        """
        
        raise NotImplementedError
    
    def prepare(self, node_name):
        """
        
        :param node_name:
        :return:
        """
        pass


"""
VOLATILE BACKEND

A non-persistent backend
"""


class VolatileBackend(Backend):
    def exists(self, node_name):
        return False
    
    def load_data(self, node_name):
        raise NotImplementedError
    
    def save_data(self, node_name, data):
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
    
    def node_path(self, node_name):
        """
        
        :param node_name:
        :return:
        """
        
        return join(self.path, '{}.{}'.format(
            node_name,
            self.ext
        ))
    
    def exists(self, node_name):
        """
        
        :param node_name:
        :return:
        """
        
        return exists(self.node_path(node_name=node_name))
    
    def load_data(self, node_name):
        """
        
        :param node_name:
        :return:
        """
        
        raise NotImplementedError
    
    def save_data(self, node_name, data):
        """
        
        :param node_name:
        :param data:
        :return:
        """
        
        raise NotImplementedError
    
    def del_node(self, node_name):
        """
        
        :param node_name:
        :return:
        """
        
        remove(self.node_path(node_name))
    
    def prepare(self, node_name):
        """
        Called before the function and save_data.
        
        :param node_name:
        :return:
        """
        node_path = self.node_path(node_name=node_name)
        self.create_path(node_path)


class JsonBackend(FileSystemBase):
    def __init__(self, path):
        """
        
        :param path:
        """
        
        super(JsonBackend, self).__init__(path, 'json')
    
    def load_data(self, node_name):
        """
        
        :param node_name:
        :return:
        """
        
        return json.load(open(self.node_path(node_name=node_name), 'r'))
    
    def save_data(self, node_name, data):
        """
        
        :param node_name:
        :param data:
        :return:
        """
        
        node_path = self.node_path(node_name=node_name)
        json.dump(data, open(node_path, 'w'))


class PickleBackend(FileSystemBase):
    def __init__(self, path):
        """

        :param path:
        """
        
        super(PickleBackend, self).__init__(path, 'pkl')
    
    def load_data(self, node_name):
        """

        :param node_name:
        :return:
        """
        
        return pickle.load(open(self.node_path(node_name=node_name), 'rb'))
    
    def save_data(self, node_name, data):
        """

        :param node_name:
        :param data:
        :return:
        """
        
        node_path = self.node_path(node_name=node_name)
        pickle.dump(data, open(node_path, 'wb'))
