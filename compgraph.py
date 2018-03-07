import pandas as pd

import warnings

from . import *


class HashableDict(dict):
    """
    
    """
    
    def __hash__(self):
        """
        
        :return:
        """
        
        return hash(frozenset(self.items()))


class ComputationGraph(object):
    def __init__(self, name=None, default_backend=None):
        """
        
        :param metadata:
        :param drop_all:
        :param default_backend:
        """
        
        self.name = name
        
        self.default_backend = default_backend
        self.backends = dict()
        self.nodes = dict()
    
    def add_backend(self, name, backend):
        """
        
        :param name:
        :param backend:
        :return:
        """
        
        self.backends[name] = backend
    
    def evaluate(self, *nodes):
        """
        
        :param nodes:
        :return:
        """
        
        for node in nodes:
            node_name = node if isinstance(node, str) else node.name
            self.nodes[node_name].evaluate()
    
    def evaluate_all(self, if_exists=False):
        """
        
        :param if_exists:
        :return:
        """
        
        for node in self.nodes.values():
            if (not node.exists()) or (node.exists() and if_exists):
                node.evaluate()
    
    def node(self, node_name, func, sources=None, kwargs=None, metadata=None, backend=None):
        """
        
        :param node_name:
        :param func:
        :param sources:
        :param kwargs:
        :param metadata:
        :param backend:
        :return:
        """
        
        if backend is None:
            backend = self.default_backend
        # elif hasattr(backend, '__len__'):
        #     assert len(backend) == 2
        
        if node_name not in self.nodes:
            self.nodes[node_name] = NodeWrapper(
                graph=self,
                node_name=node_name,
                metadata=metadata,
                func=func,
                kwargs=kwargs,
                sources=sources,
                backend=(backend, self.backends[backend])
            )
        
        return self.nodes[node_name]
    
    def render_graph(self, root=None, view=True, split_keys=False, max_len=40):
        """

        :param root:
        :param view:
        :param max_len:
        :return:
        """
        
        from graphviz import Digraph
        
        dot = Digraph(
            name=self.name,
            comment=self.name,
            graph_attr=dict(
                splies='line',
                # nodesep='1',
                rankdir='TB',
            )
        )
        
        max_node = 1e12
        node_ids = dict()
        
        def get_or_create_node(node_name):
            if node_name not in node_ids:
                node_ids[node_name] = len(node_ids)
                
                short_node_name = node_name
                if len(node_name) > max_len:
                    short_node_name = node_name[:max_len] + '...'
                    
                dot.node(
                    name=str(node_ids[node_name]),
                    label=[short_node_name, short_node_name.split('/')[-1]][split_keys],
                    style=['filled', None][self.nodes[node_name].exists]
                )
        
        for sink_name, sink in self.nodes.items():
            max_node += 1
            
            # factor_name = str(max_node)
            # factor_label = '{}({})'.format(
            #     sink.node.func,
            #     ', '.join(['{}={}'.format(kk, vv) for kk, vv in sink.node.kwargs.items()])
            # )
            
            # dot.node(
            #     name=factor_name,
            #     label='',
            #     style='filled',
            #     _attributes=dict(
            #         shape='point'
            #     )
            # )
            
            get_or_create_node(sink.node_name)
            
            # dot.edge(
            #     tail_name=factor_name,
            #     head_name=str(sink.node.id),
            #     taillabel=factor_label,
            # )
            
            for sn, source in (sink.sources or {}).items():
                get_or_create_node(source.node_name)
                
                dot.edge(
                    tail_name=str(node_ids[source.node_name]),
                    head_name=str(node_ids[sink.node_name]),  # factor_name
                    fontsize='8',
                    # arrowhead='none'
                )
        
        import subprocess
        try:
            dot.render(directory=root, view=view)
        except subprocess.CalledProcessError:
            return


class BackendManager(object):
    def __init__(self):
        """
        
        """
        
        self.backends = dict()
    
    def add_backend(self, key, backend):
        """
        
        :param key:
        :param backend:
        :return:
        """
        
        self.backends[key] = backend
        setattr(self, key, backend)
    
    def del_backend(self, key):
        """
        
        :param key:
        :return:
        """
        
        del self.backends[key]
        delattr(self, key)


class NodeWrapper(object):
    def __init__(self, graph, node_name, metadata, func, sources, kwargs, backend, autoextract=False):
        """
        
        :param node_name:
        :param metadata:
        :param func:
        :param sources:
        :param kwargs:
        :param serialise:
        """
        
        self.graph = graph
        self.func = func
        self.kwargs = kwargs
        
        # Validate source types
        if sources is not None:
            if isinstance(sources, NodeWrapper):
                sources = {sources.name: sources}
            elif isinstance(sources, (list, tuple)):
                sources = {src.name: src for src in sources}
            else:
                assert isinstance(sources, dict), type(sources)
                for source in sources.values():
                    assert isinstance(source,
                                      NodeWrapper), ('Often, there has been a mixup between a source '
                                                     'and kwarg parameter when instantiating the node')
        
        self.sources = sources
        self.node_name = node_name
        
        # # Get or create the node
        # # TODO: remove dependency on peewee
        # query = Node.select().where(
        #     Node.dataset == dataset,
        #     Node.name == node_name,
        # )
        
        self.backend_name, self.backend = backend
        
        # Get or create the source
        # if query.count() == 0:
        #     try:
        #         # TODO: remove dependency on peewee
        #         self.node = Node.create(
        #             dataset=dataset,
        #             name=node_name,
        #             func=self.func.__name__,
        #             metadata=metadata or {},
        #             kwargs=kwargs or {},
        #             backend=self.backend_name,
        #         )
        #     except TypeError:
        #         # TODO: remove dependency on peewee
        #         self.node = Node.create(
        #             dataset=dataset,
        #             name=node_name,
        #             func=self.func.__name__,
        #             metadata=metadata or {},
        #             kwargs={},
        #             backend=self.backend_name,
        #         )
        
        # else:
        #     # TODO: remove dependency on peewee
        #     self.node = Node.get(
        #         Node.dataset == dataset,
        #         Node.name == node_name,
        #     )
        
        # if self.sources and len(self.sources):
        #     for source in self.sources.values():
        #         # TODO: remove dependency on peewee
        #         Edges.get_or_create(
        #             source=source.node,
        #             sink=self.node,
        #         )
        #
        # if self.sources is None or len(self.sources) == 0:
        #     if not self.exists and autoextract:
        #         hashable_kwargs, hashable_sources = self.hashables()
        #         compute_or_load_evaluation(
        #             node=self.node,
        #             sources=hashable_sources,
        #             kwargs=hashable_kwargs,
        #             func=self.func,
        #             backend=self.backend,
        #         )
    
    @property
    def exists(self):
        """
        
        :return:
        """
        
        return self.backend.exists(self.node_name)
    
    @property
    def name(self):
        """
        
        :return:
        """
        
        return self.node_name
    
    def __repr__(self):
        """
        
        :return:
        """
        
        return '<{} sources=[{}] factor={} sink={}>'.format(
            self.__class__.__name__,
            ','.join(self.sources.keys()) if self.sources else '',
            self.func.__name__,
            self.node_name
        )
    
    def hashables(self):
        """
        
        :return:
        """
        
        hashable_sources = HashableDict(self.sources or {})
        hashable_kwargs = {}
        for kk, vv in (self.kwargs or {}).items():
            if isinstance(vv, list):
                hashable_kwargs[kk] = tuple(vv)
            elif isinstance(vv, (int, float, str)):
                hashable_kwargs[kk] = vv
            else:
                print(kk, type(vv))
                hashable_kwargs[kk] = vv
                # raise NotImplementedError
        hashable_kwargs = HashableDict(hashable_kwargs)
        
        return hashable_kwargs, hashable_sources
    
    def get_data(self):
        """
        
        :return:
        """
        
        raise NotImplementedError
    
    def evaluate(self):
        """
        Check if exists in Data model, and load if does.

        :param args:
        :param kwargs:
        :return:
        """
        
        hashable_kwargs, hashable_sources = self.hashables()
        
        res = compute_or_load_evaluation(
            node_name=self.node_name,
            sources=hashable_sources,
            kwargs=hashable_kwargs,
            func=self.func,
            backend=self.backend,
        )
        
        self.graph.render_graph(
            root=self.graph.fs_root,
            view=False,
            split_keys=True
        )
        
        return res


def compute_or_load_evaluation(node_name, sources, kwargs, func, backend):
    """
    
    :param node:
    :param sources:
    :param kwargs:
    :param func:
    :param backend:
    :return:
    """
    
    if node_name in compute_or_load_evaluation.cache:
        return compute_or_load_evaluation.cache[node_name]
    
    if not backend.exists(node_name=node_name):
        inputs = (kwargs or {}).copy()
        if sources and len(sources):
            for key, value in sources.items():
                inputs[key] = value.evaluate()
        
        print('Calculating {}'.format(node_name))
        
        data = func(**inputs)
        backend.save_data(node_name=node_name, data=data)
    
    else:
        print('Acquiring {}'.format(node_name))
        data = backend.load_data(node_name=node_name)
    
    compute_or_load_evaluation.cache[node_name] = data
    
    return data


if not hasattr(compute_or_load_evaluation, 'cache'):
    compute_or_load_evaluation.cache = {}
