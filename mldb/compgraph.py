from functools import partial
from os import environ
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from uuid import uuid4

from loguru import logger

from mldb.backends import Backend
from mldb.backends import VolatileBackend


class ComputationGraph(object):
    """
    This class defines a lightweight interface for lazy computation, serialisation, de-serialisation
    and local caching of data. With this class, one can define complex computational chains that
    begin with raw data (eg from file) and output predictive models and performance evaluation
    metrics that may be saved to file if desired.

    The following illustrates basic usage of the library. The `load_data` function itself would
    ordinarily perform a more complex operation than returning such a small amount of data. For
    example, it may load a whole dataset and set of labels to file.

    >>> def load_data():
    ...     return [[1, 2, 3], [4, 5, 6]]
    ...
    >>> from mldb import ComputationGraph
    >>> graph = ComputationGraph()
    >>> node = graph.make_node(func=load_data)
    >>> make_node
    <NodeWrapper sources=[] kwargs=[] factor=load_data sink=d16d98ef-2efc-41ef-b2e5-96738689c970>
    >>> make_node.evaluate()
    [[1, 2, 3], [4, 5, 6]]

    Line 4 defines a NodeWrapper object that is evaluated on Line 6. Note, the name for the node is
    given by the UUID package. An optional `name` parameter should be added if the name is to be
    identifiable.

    More complex relationships can be encoded. For example, the maximum over the rows of the data
    returned from `load_data` can be extracted by:

    >>> def load_data():
    ...     return [[1, 2, 3], [4, 5, 6]]
    ...
    >>> def max_row(data):
    ...     return [max(row) for row in data]
    ...
    >>> from mldb import ComputationGraph
    >>> graph = ComputationGraph()
    >>> node = graph.make_node(func=load_data)
    >>> max_node = graph.make_node(func=max_row, kwargs=dict(data=make_node))
    >>> max_node
    <NodeWrapper sources=[data] kwargs=[] factor=max_row sink=3d7a5b7d-48cc-4010-b1cc-05ad348714e0>
    >>> max_node.evaluate()
    [3, 6]

    Note, that it was not necessary to directly call `evaluate()` on the node object. This is because the
    internal logic of the `evaluate` member function traverse the computational graph and automatically
    evaluates the intermediate nodes.

    This library allows additional (non-NodeWrapper) key words to be passed into the function too. In the
    following example, the function multiplies the data by a particular value `x` that is not known in
    advance. The value of `x` is specified in the `kwargs` argument of ComputationGraph.node (as opposed to
    the sources argument) since one does not expect kwargs to contain NodeWrapper objects.

    >>> def load_data():
    ...     return [[1, 2, 3], [4, 5, 6]]
    ...
    >>> def max_row(data):
    ...     return [max(row) for row in data]
    ...
    >>> def scale_data(data, scale):
    ...     return [d * scale for d in data]
    ...
    >>> from mldb import ComputationGraph
    >>> graph = ComputationGraph()
    >>> node = graph.make_node(func=load_data)
    >>> max_node = graph.make_node(func=max_row, kwargs=dict(data=make_node)()
    >>> max_node_times_3 = graph.make_node(func=scale_data, kwargs=dict(data=max_node, scale=3))
    >>> max_node_times_3
    <NodeWrapper sources=[data] kwargs=[scale] factor=scale_data sink=f1b3d6c0-deab-430a-9c6f-cdd6f2ff38e4>
    >>> max_node_times_3.evaluate()
    [9, 18]
    """

    def __init__(self, name: Optional[str] = None):
        """
        A computational graph that acts as a container for `NodeWrapper`s.

        Parameters
        ----------
        name: Optional[str] (default=None)
            This optional parameter gives a name to the collection of nodes. If None, a random uuid64
            string will be generated for the collection.

        """

        if name is None:
            name = str(uuid4())

        self.name: str = name.lower()
        self.nodes: Dict[Any, NodeWrapper] = dict()

    def __repr__(self) -> str:
        """Represent the graph string"""

        name = self.name
        nodes = sorted(map(str, self.nodes.keys()))

        return f"{self.__class__.__name__}({name=}, {nodes=})"

    def evaluate(self, force: bool = False) -> Dict[Any, Any]:
        """
        Iterate through all nodes and evaluate their values.

        Parameters
        ----------
        force: bool (default=False)

        Returns
        -------
        None
        """

        evaluations = dict()

        for key, node in self.nodes.items():
            if node.exists and not force:
                continue
            logger.info(f"Evaluating {key}...")
            evaluations[key] = node.evaluate()

        return evaluations

    def make_node(
        self,
        func: Callable,
        name: Optional[Any] = None,
        key: Optional[Any] = None,
        args: Optional[Union[Any, List[Any], Tuple[Any]]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        backend: Optional[Backend] = None,
        cache: bool = True,
        collect: bool = True,
    ) -> "NodeWrapper":
        """
        This function generates a new Node wrapper

        Parameters
        ----------
        func: Callable
            This is the function that is to be wrapped.
        name: Optional[str] (default=None)
            This is the name of the node that is to be created. If `cache is True`, the node
            will be stored under `ComputationGraph.nodes[name]`
        key: Optional[str] (default=None)
            An optional key with which to store the node. If `key=None`, the `name` argument is used as key.
        args: Optional[Union[List[Any], Tuple[Any]]] (default=None)
            The args to be passed into `func`.
        kwargs: Optional[Dict[str, Any]] (default=None)
            The kwargs to be passed into `func`.
        backend: Optional[Backend] (default=None)
            The optional backend used for serialising/deserialising
        cache: bool (default=True)
            Whether to use a backend
        collect: bool (default=True)
            Whether to add the node to `self.nodes`

        Returns
        -------
        node: NodeWrapper
            This is the newly created lazy evaluation object

        """

        assert callable(func)

        # Validate the name/key
        if name is None:
            name = str(uuid4())
        if name in self.nodes:
            raise KeyError(f"A node with '{name}' name already exists in {self.nodes.keys()}.")
        if key is None:
            key = name

        # Validate backend
        if backend is not None and not isinstance(backend, Backend):
            raise ValueError(f"The backend is expected to be of type Backend, but got {type(backend)}.")
        if not cache:
            backend = None

        # Validate keywords
        if kwargs is not None and not isinstance(kwargs, dict):
            raise ValueError(f"")

        # Validate args
        if args is not None and not isinstance(args, tuple):
            args = (args,)

        # Instantiate the wrapper
        node = NodeWrapper(graph=self, name=name, func=func, backend=backend, args=args, kwargs=kwargs)

        # Save to nodes
        if collect:
            self.nodes[key] = node

        return node


def get_function_name(func: Callable) -> str:
    """This function takes a callable function and attempts to extract a string name for it."""

    if isinstance(func, partial):
        func = func.func.__self__.func

    return func.__name__


class NodeWrapper(object):
    def __init__(
        self,
        graph: ComputationGraph,
        name: Any,
        func: Callable,
        backend: Backend,
        args: Optional[Tuple[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        This class provides the light wrapper around lazy evaluation/serialisation/de-serialisation with the
        func, kwargs, and backend

        Parameters
        ----------
        graph: ComputationGraph
            The parent computational graph of which this node is a child.
        name: str
            The name of the node
        func: Callable
            The function that will be evaluated, and whose output is cached via the backend.v
        kwargs: Optional[Dict[str, Any]]
            The keywords for the function.
        backend: Backend
            A Backend instance that facilitates serialisation/de-serialisation of data
        """

        self.graph: "ComputationGraph" = graph

        self.name: str = name

        self.func: Callable = func
        self.args: Tuple[Any] = tuple() if args is None else args
        self.kwargs: Dict[str, Any] = dict() if kwargs is None else kwargs

        self.backend: Backend = backend

    def __repr__(self) -> str:
        """Representation of NodeWrapper object"""

        func = get_function_name(self.func)
        args = [arg.name if isinstance(arg, NodeWrapper) else arg for arg in self.args]
        kwargs = sorted(map(str, self.kwargs.keys()))

        return f"{self.__class__.__name__}({func=}, {args=}, {kwargs=})"

    @property
    def exists(self) -> bool:
        """A wrapper around the backend object to determine if the computation of `self.func` exists already."""

        return self.backend.get(self.name).exists()

    @property
    def sources(self) -> Dict[str, "NodeWrapper"]:
        """A convenience function to get the sources (i.e. the arguments for `self.func` that are `NodeWrapper`'s)."""

        return {kk: vv for kk, vv in self.kwargs.items() if isinstance(vv, NodeWrapper)}

    @property
    def keywords(self) -> Dict[str, Any]:
        """A convenience function to get non`NodeWrapper` keyword arguments for `self.func`."""

        return {kk: vv for kk, vv in self.kwargs.items() if not isinstance(vv, NodeWrapper)}

    def evaluate(self) -> Any:
        """
        This function evaluates `self.func` with `self.kwargs`. If this has already been called, the cached version
        (from an LRU cache) or the serialised version (via the backend) will be returned instead. If the evaluation
        has not been done before, the evaluation will be

        Returns
        -------
        Any: the return value of `self.func`.
        """

        return compute_or_load_evaluation(
            name=self.name, func=self.func, backend=self.backend, args=self.args, kwargs=self.kwargs
        )


def resolve_arguments(arguments):
    if isinstance(arguments, NodeWrapper):
        return arguments.evaluate()
    if isinstance(arguments, (list, tuple)):
        return type(arguments)(resolve_arguments(val) for val in arguments)
    elif isinstance(arguments, dict):
        return {kk: resolve_arguments(vv) for kk, vv in arguments.items()}
    return arguments


def compute_or_load_evaluation(
    name: str, func: Callable, backend: Backend, args: Optional[Tuple[Any]], kwargs: Optional[Dict[str, Any]]
):
    """
    This function is the main workhorse of the library, and manages the backends, function and cache.

    Parameters
    ----------
    name: str
        Name of the node object to he calculated/loaded/returned from cache
    func: callable
        The function to call to evaluate the node value, if not cached already.
    backend: Backend
        The backend interface
    args: Optional[Tuple[Any]]
        The args for func
    kwargs: Optional[Dict[str, Any]]
        The keywords for func

    Returns
    -------
    Any:
        The return value of `func`, either calculated new, or loaded from cache/file as required.

    """

    # Built a short name for printing purposes
    if isinstance(name, Path):
        name_short = name.resolve().relative_to(environ.get("BUILD_ROOT", "/"))
    else:
        name_short = name

    if len(name_short) > 75:
        name_short = f"...{name_short[-50:]}"

    # If the data is cached, return it
    if name in compute_or_load_evaluation.cache:
        logger.info(f"Loading {name_short} from local cache")
        return compute_or_load_evaluation.cache[name]

    if backend is None:
        backend = VolatileBackend()
    backend_interface = backend.get(name)

    if not backend_interface.exists():
        # Set default args and kwargs
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()

        with backend_interface.lock():
            # Build up dictionary of inputs
            args = resolve_arguments(args)
            kwargs = resolve_arguments(kwargs)

            # Calculate the output
            logger.info(f"Evaluating {name_short}...")
            try:
                data = func(*args, **kwargs)
            except Exception as ex:
                logger.exception(f"The following exception was raised when computing {name}: {ex}")
                raise ex

            # Save data if not None
            if data is not None and backend.cache_data:
                logger.info(f"Serialising {name_short}...")
                try:
                    backend_interface.save(data=data)
                except Exception as ex:
                    logger.exception(f"The following exception was raised when saving {name}: {ex}")
                    raise ex

    else:
        try:
            logger.info(f"Deserialising {name_short}...")
            data = backend_interface.load()
        except Exception as ex:
            logger.exception(f"The following exception was raised when loading {name}: {ex}")
            raise ex

    # Determine whether to cache these results or not
    compute_or_load_evaluation.cache[name] = data

    return data


if not hasattr(compute_or_load_evaluation, "cache"):
    # TODO/FIXME: make a LRU cache with pre-defined storage capacity instead
    compute_or_load_evaluation.cache = {}


if __name__ == "__main__":

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
            return "long process completed"

        graph = ComputationGraph()
        node = graph.make_node(func=load)
        node_max = graph.make_node(func=max_row, kwargs=dict(data=node))
        x3 = graph.make_node(func=times_x, kwargs=dict(data=node_max, x=3))

        long = graph.make_node(func=long_process, name="long_process_name")

        print(node.evaluate())
        print(node_max.evaluate())
        print(x3.evaluate())
        print(node)
        print(node_max)
        print(x3)
        print(long.evaluate())

    main()
