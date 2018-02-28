from os.path import exists, join
from os import makedirs, remove

import json
import pickle

from .mldb import *


class Backend(object):
    def __init__(self):
        """
        
        """
        
        pass
    
    def exists(self, node):
        """
        
        :param node:
        
        :return:
        """
        
        raise NotImplementedError
    
    def load_data(self, node):
        """
        
        :param node:
        
        :return:
        """
        
        raise NotImplementedError
    
    def save_data(self, node, data):
        """
        
        :param node:
        
        :param data:
        :return:
        """
        
        raise NotImplementedError
    
    def del_node(self, node):
        """
        
        :param node:
        
        :return:
        """
        
        raise NotImplementedError


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
        path_split = path.split('/')
        if len(path_split) > 1:
            path_join = '/'.join(path_split[:-1])
            if not exists(path_join):
                makedirs(path_join)
    
    def node_path(self, node):
        """
        
        :param node:
        :return:
        """
        
        return join(self.path, '{}.{}'.format(
            node.name,
            self.ext
        ))
    
    def exists(self, node):
        """
        
        :param node:
        :return:
        """
        
        return exists(self.node_path(node=node))
    
    def load_data(self, node):
        """
        
        :param node:
        :return:
        """
        
        raise NotImplementedError
    
    def save_data(self, node, data):
        """
        
        :param node:
        :param data:
        :return:
        """
        
        raise NotImplementedError
    
    def del_node(self, node):
        """
        
        :param node:
        :return:
        """
        
        remove(self.node_path(node))


class JsonBackend(FileSystemBase):
    def __init__(self, path):
        """
        
        :param path:
        """
        
        super(JsonBackend, self).__init__(path, 'json')
    
    def load_data(self, node):
        """
        
        :param node:
        :return:
        """
        
        return json.load(open(self.node_path(node=node), 'r'))
    
    def save_data(self, node, data):
        """
        
        :param node:
        :param data:
        :return:
        """

        node_path = self.node_path(node=node)
        self.create_path(node_path)
        json.dump(data, open(node_path, 'w'))


class PickleBackend(FileSystemBase):
    def __init__(self, path):
        """
        
        :param path:
        """
        
        super(PickleBackend, self).__init__(path, 'pkl')
    
    def load_data(self, node):
        """
        
        :param node:
        :return:
        """
        
        return pickle.load(open(self.node_path(node=node), 'rb'))
    
    def save_data(self, node, data):
        """
        
        :param node:
        :param data:
        :return:
        """

        node_path = self.node_path(node=node)
        self.create_path(node_path)
        pickle.dump(data, open(node_path, 'wb'))


"""
DATABASE BACKENDS
"""


class PostgresBackend(Backend):
    def __init__(self):
        """
        
        """
        
        super(PostgresBackend, self).__init__()
    
    def exists(self, node):
        """
        
        :param node:
        :return:
        """
        
        return Data.select().where(Data.node == node).exists()
    
    def load_data(self, node):
        """
        
        :param node:
        :return:
        """
        
        query = Data.select().where(
            Data.node == node
        ).dicts()
        
        return [row['d'] for row in query]
    
    def save_data(self, node, data):
        """
        
        :param node:
        :param data:
        :return:
        """
        
        assert isinstance(data, list) and isinstance(data[0], dict)
        Data.insert_many([dict(node=node, d=dd) for di, dd in data.iterrows()]).execute()
    
    def del_node(self, node):
        """
        
        :param node:
        :return:
        """
        
        Data.delete().where(Data.node == node).execute()
