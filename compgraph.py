from os import environ

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
        
        self.name = name.lower()
        self.backends = dict()
        self.nodes = dict()
        
        self.default_backend = default_backend
        self.set_default_backend(default_backend)
    
    def __repr__(self):
        """
        
        :return:
        """
        return f"<{self.__class__.__name__} name={self.name}>"
    
    def add_backend(self, name, backend, default=False):
        """
        
        :param name:
        :param backend:
        :param default:
        :return:
        """
        
        assert name not in self.backends
        
        self.backends[name] = backend
        if default:
            self.set_default_backend(backend)
    
    def set_default_backend(self, backend):
        """
        
        :param backend:
        :return:
        """
        
        if backend is None:
            return
        if backend not in self.backends:
            raise KeyError(
                f'The backend {backend} is not found in {{{self.backends.keys()}}}'
            )
        self.default_backend = backend
    
    def evaluate_all_nodes(self, force=False):
        """
        
        :param force:
        :return:
        """
        
        for node in self.nodes.values():
            if (not node.exists) or (node.exists and force):
                node.evaluate()
    
    def node(self, node_name, func, sources=None, kwargs=None, backend=None):
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
        
        if node_name not in self.nodes:
            self.nodes[node_name] = NodeWrapper(
                graph=self,
                node_name=node_name,
                func=func,
                kwargs=kwargs,
                sources=sources,
                backend=self.backends[backend],
            )
        
        return self.nodes[node_name]


class NodeWrapper(object):
    def __init__(self, graph, node_name, func, sources, kwargs, backend):
        """
        
        :param graph:
        :param node_name:
        :param func:
        :param sources:
        :param kwargs:
        :param backend:
        """
        
        self.graph = graph
        self.func = func
        self.kwargs = kwargs
        
        # Validate source types
        if sources is not None:
            assert isinstance(sources, dict), type(sources)
            for source in sources.values():
                if not isinstance(source, NodeWrapper):
                    raise ValueError(
                        f'All `sources` arguments must be of type `NodeWrapper but '
                        f'{source} is of type {type(source)}. This can sometimes occur '
                        f'when `kwargs` and `sources` arguments are mixed up.'
                    )
        
        self.sources = sources
        self.backend = backend
        self.node_name = node_name
    
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
        
        try:
            func_name = self.func.__name__
        except AttributeError:
            func_name = 'functools.partial'
        
        return '<{} sources=[{}] factor={} sink={}>'.format(
            self.__class__.__name__,
            ','.join(self.sources.keys()) if self.sources else '',
            func_name,
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
                # print(kk, type(vv))
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
        
        return res


def compute_or_load_evaluation(node_name, sources, kwargs, func, backend):
    """

    :param node_name:
    :param sources:
    :param kwargs:
    :param func:
    :param backend:
    :return:
    """
    
    # If the data is cached, return it
    if node_name in compute_or_load_evaluation.cache:
        return compute_or_load_evaluation.cache[node_name]
    
    # Built a short name for printing purposes
    node_name_short = node_name
    if 'BUILD_ROOT' in environ:
        if node_name.startswith(environ['BUILD_ROOT']):
            node_name_short = node_name[len(environ['BUILD_ROOT']):]
    
    if not backend.exists(node_name=node_name):
        # Ensure that the sources and keywords have no overlapping members
        kwargs = (kwargs or dict())
        sources = (sources or dict())
        if len(set(kwargs.keys()) & set(sources.keys())) > 0:
            raise ValueError(
                f'Source and keywords arguments cannot overlap; with '
                f'sources: {{{sources.keys()}}}; keywords: {{{kwargs.keys()}}}'
            )
        
        # Build up dictionary of inputs
        inputs = kwargs.copy()
        if sources and len(sources):
            for key, value in sources.items():
                inputs[key] = value.evaluate()
        
        backend.prepare(node_name=node_name)
        
        # Calculate the output
        print('Calculating {}'.format(node_name_short))
        try:
            data = func(**inputs)
        except Exception as ex:
            raise ex
        
        # Save data if not None
        if data is not None:
            backend.save_data(node_name=node_name, data=data)
    
    else:
        data = backend.load_data(node_name=node_name)

    # Determine whether to cache these results or not
    cache_data = hasattr(backend, 'cache_data') and backend.cache_data
    if cache_data and data is not None:
        compute_or_load_evaluation.cache[node_name] = data
    
    return data


if not hasattr(compute_or_load_evaluation, 'cache'):
    compute_or_load_evaluation.cache = {}
