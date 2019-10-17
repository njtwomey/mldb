from os import environ
from uuid import uuid4

from mldb.backends import VolatileBackend
from mldb.logger import get_logger

logger = get_logger(__name__)


class ComputationGraph(object):
    """
    This class defines a lightweight interface for lazy computation, serialisation, de-serialisation
    and local caching of data. With this class, one can define complex computational chains that
    begin with raw data (eg from file) and output predictive models and performance evaluation
    metrics that may be saved to file if desired.
    
    The following illustrates basic usage of the library. The `load_data` function itself would
    ordinarily perform a more complex operation than returning such a small amount of data. For
    example, it may load a whole dataset and set of labels to file.
    
    ```python
    In [1]: from mldb import ComputationGraph
    In [2]: graph = ComputationGraph()
    In [3]: def load_data():
       ...:     return [[1, 2, 3], [4, 5, 6]]
       ...:
    In [4]: node = graph.node(func=load_data)
    In [5]: node
    Out[5]: <NodeWrapper sources=[] kwargs=[] factor=load sink=8603973e-1e51-45d9-9470-2e7f37a27054
    In [6]: node.evaluate()
    Out[6]: [[1, 2, 3], [4, 5, 6]]
    ```
    
    Line 4 defines a NodeWrapper object that is evaluated on Line 6. Note, the name for the node is
    given by the UUID package. An optional `name` parameter should be added if the name is to be
    identifiable.
    
    More complex relationships can be encoded. For example, the maximum over the rows of the data
    returned from `load_data` can be extracted by:
    
    ```python
    In [1]: from mldb import ComputationGraph
    In [2]: graph = ComputationGraph()
    In [3]: def load_data():
       ...:     return [[1, 2, 3], [4, 5, 6]]
       ...:
    In [4]: def max_row(data):
       ...:     return list(map(max, data))
       ...:
    In [5]: node = graph.node(func=load_data)
    In [6]: max_node = graph.node(func=lambda data: list(map(max, data)), data=node)
    In [7]: max_node
    Out[7]: <NodeWrapper sources=[data] kwargs=[] factor=max_row sink=9bf467a4-0878-44a1-9ed1-f1956a9826ae>
    In [8]: max_node.evaluate()
    Out[8]: [3, 6]
    ```
    
    Note, that it was not necessary to directly call `evaluate()` on the node object. This is because the
    internal logic of the `evaluate` member function traverse the computational graph and automatically
    evaluates the intermediate nodes.
    
    This library allows additional (non-NodeWrapper) key words to be passed into the function too. In the
    following example, the function multiplies the data by a particular value `x` that is not known in
    advance. The value of `x` is specified in the `kwargs` argument of ComputationGraph.node (as opposed to
    the sources argument) since one does not expect kwargs to contain NodeWrapper objects.
    
    ```python
    In [9]: def times_x(data, x):
       ...:     return [d * x for d in data]
       ...:
    In [10]: max_node_times_3 = graph.node(func=times_x, data=max_node, x=3)
    In [11]: max_node_times_3
    Out[11]: <NodeWrapper sources=[data] kwargs=[x] factor=times_x sink=3de46ba9-3199-486a-9bd1-a86c1a01ee6a>
    In [12]: max_node_times_3.evaluate()
    Out[12]: [9, 15]
    ```
    
    """
    
    def __init__(self, name=None):
        """
        
        Args:
            name: str, None
                This defines the name of the graph.
        """
        
        if name is None:
            name = str(uuid4())
        
        self.name = name.lower()
        self.backends = dict()
        self.nodes = dict()
        
        self.default_backend = None
        
        self.add_backend('none', VolatileBackend(), default=True)
    
    def __repr__(self):
        """
        
        Returns:

        """
        
        return f"<{self.__class__.__name__} name={self.name}>"
    
    def add_backend(self, name, backend, default=False):
        """
        
        Args:
            name:
            backend:
            default:

        Returns:

        """
        
        logger.info(f"Adding backend {name} to list (currently: {self.backends.keys()})")
        if name in self.backends:
            msg = f"The backend {name} has already been added: {self.backends.keys()}"
            logger.critical(msg)
            raise KeyError(msg)
        
        self.backends[name] = backend
        if default:
            self.set_default_backend(name)
    
    def set_default_backend(self, backend_name):
        """
        
        Args:
            backend_name:

        Returns:

        """
        
        logger.info(f"Setting {backend_name} as the default backend.")
        
        if backend_name is None:
            logger.warn(f"Attempted to set new default backend, but specified backend is None ({backend_name})")
            return
        
        if not isinstance(backend_name, str):
            msg = f"The backend_name parameter must be a string. Got {type(backend_name)} as {backend_name}"
            logger.critical(msg)
            raise TypeError(msg)
        
        if backend_name not in self.backends.keys():
            msg = f'The backend {backend_name} is not found in {{{self.backends.keys()}}}'
            logger.critical(msg)
            raise KeyError(msg)
        
        self.default_backend = backend_name
    
    def evaluate_all_nodes(self, force=False):
        """
        
        Args:
            force:

        Returns:

        """
        
        for node in self.nodes.values():
            if node.exists:
                if not force:
                    continue
            logger.info(f"Evaluating {node.name}")
            node.evaluate()
    
    def node(self, func, name=None, backend=None, **kwargs):
        """
        
        Args:
            func:
            name:
            backend:
            **kwargs:

        Returns: NodeWrapper object
            A wrapper around the computation of the node object.
        Raises:
            ValueError: if backend is None and the default backend hasn't been specified.
            KeyError: if the key 'name' is not found in the set of computed nodes.
        """
        
        if name is None:
            name = str(uuid4())
        if backend is None:
            backend = self.default_backend
        if backend is None:
            if not len(self.backends):
                msg = f"No backends have been defined in the ComputationalGraph, terminating."
                logger.critical(msg)
                raise ValueError(msg)
            else:
                logger.info(f"Defaulting to the first backend as default: {self.backends.keys()}")
                self.set_default_backend(next(self.backends.keys()))
                backend = self.default_backend
        
        if name not in self.nodes:
            logger.info(f"Adding {name} to node list")
            self.nodes[name] = NodeWrapper(
                graph=self,
                name=name,
                func=func,
                backend=self.backends[backend],
                **kwargs
            )
        
        else:
            msg = f"An attempt to add node {name} was made, but this node already exists"
            logger.critical(msg)
            raise KeyError(msg)
        
        return self.nodes[name]


class NodeWrapper(object):
    def __init__(self, graph, name, func, backend, **kwargs):
        """
        
        Args:
            graph:
            name:
            func:
            backend:
            **kwargs:
        """
        
        self.graph = graph
        
        self.name = name
        
        self.kwargs = kwargs
        self.func = func
        
        self.backend = backend
    
    @property
    def exists(self):
        """
        
        Returns:

        """
        
        return self.backend.get(self.name).exists()
    
    @property
    def sources(self):
        """
        
        Returns:

        """
        return {
            kk: vv for kk, vv in self.kwargs.items() if isinstance(vv, NodeWrapper)
        }
    
    @property
    def keywords(self):
        """
        
        Returns:

        """
        return {
            kk: vv for kk, vv in self.kwargs.items() if not isinstance(vv, NodeWrapper)
        }
    
    def __repr__(self):
        """
        
        Returns:

        """
        
        try:
            func_name = self.func.__name__
        except AttributeError:
            func_name = '__unavailable__'
        
        return '<{} sources=[{}] kwargs=[{}] factor={} sink={}>'.format(
            self.__class__.__name__,
            ','.join(self.sources.keys()) if self.sources else '',
            ','.join(self.keywords.keys()) if self.sources else '',
            func_name,
            self.name
        )
    
    def evaluate(self):
        """
        
        Returns:

        """
        
        # TODO: Consider adding backend.reserve so that independent processes won't concurrently evaluate
        
        res = compute_or_load_evaluation(
            name=self.name,
            func=self.func,
            backend=self.backend,
            **self.kwargs
        )
        
        return res


def compute_or_load_evaluation(name, func, backend, **kwargs):
    """
    
    Args:
        name:
        func:
        backend:
        **kwargs:

    Returns:

    """
    
    # If the data is cached, return it
    if name in compute_or_load_evaluation.cache:
        logger.info(f"Loading cached result for {name}")
        return compute_or_load_evaluation.cache[name]
    
    # Built a short name for printing purposes
    name_short = (
        name[len(environ['BUILD_ROOT']):] if (
            'BUILD_ROOT' in environ and
            name.startswith(environ['BUILD_ROOT'])
        ) else name
    )
    
    backend_interface = backend.get(name)
    
    if not backend_interface.exists():
        with backend_interface.lock():
            # Build up dictionary of inputs
            inputs = kwargs.copy()
            for key, value in kwargs.items():
                if isinstance(value, NodeWrapper):
                    inputs[key] = value.evaluate()
            
            # Calculate the output
            logger.info(f'Calculating {name_short}')
            try:
                data = func(**inputs)
            except Exception as ex:
                logger.exception(f"The following exception was raised when computing {name}: {ex}")
                raise ex
            
            # Save data if not None
            if data is not None:
                try:
                    backend_interface.save(data=data)
                except Exception as ex:
                    logger.exception(f"The following exception was raised when saving {name}: {ex}")
                    raise ex
    
    else:
        try:
           data = backend_interface.load()
        except Exception as ex:
            logger.exception(f"The following exception was raised when loading {name}: {ex}")
            raise ex
    
    # Determine whether to cache these results or not
    cache_data = hasattr(backend, 'cache_data') and backend.cache_data
    if cache_data and data is not None:
        compute_or_load_evaluation.cache[name] = data
    
    return data


if not hasattr(compute_or_load_evaluation, 'cache'):
    # TODO/FIXME: make a LRU cache with pre-defined storage capacity instead
    compute_or_load_evaluation.cache = {}

if __name__ == '__main__':
    def main():
        from time import sleep
        
        def load():
            return [[1, 2, 3], [4, 5, 6]]
        
        def max_row(data):
            return list(map(max, data))
        
        def times_x(data, x):
            return [d * x for d in data]
        
        def long_process():
            sleep(10)
            return 'long process completed'
        
        graph = ComputationGraph()
        node = graph.node(func=load)
        node_max = graph.node(func=max_row, data=node)
        x3 = graph.node(func=times_x, data=node_max, x=3)
        
        long = graph.node(func=long_process, name='long_process_name')
        
        print(node.evaluate())
        print(node_max.evaluate())
        print(x3.evaluate())
        print(node)
        print(node_max)
        print(x3)
        print(long.evaluate())
    
    
    main()
