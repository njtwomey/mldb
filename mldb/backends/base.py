from pathlib import Path
from typing import Any
from typing import Type

from mldb.locker import FileLock
from mldb.locker import FileLockExistsException

__all__ = [
    "BackendInterface",
    "Backend",
]


class BackendInterface(object):
    """
    This abstract class provides the main functions that need to be implemented for a backend:

    * exists()
    * load()
    * save()
    * delete()
    * lock()

    Each of these member functions must be specialised by child backend objects. For example, if
    considering a file-system backend, the `exists` function could test the whether a file
    exists. Backends are primarily designed to provide guarantees of the serialised data types.

    The lock function is not abstract and it creates a lockfile object. The scope of the lock is
    best managed with a with statement. For example, a file called `test.lock` will be created
    upon `__enter__` and this will be unlinked when `__exit__` is called. If the file `test.lock`
    already exists, a `mldb.locker.FileLockExistsException` will be raised.

    ```python
    interface = BackendInterface(name='test')
    with interface.lock():
        pass
    ```

    """

    def __init__(self, path: str, *args, **kwargs):
        """
        Instantiate the backend interface.

        Parameters
        ----------
            path: string
                The argument `path` is a string that uniquely defines the node that is being
                queried.
            *args: *iterable
                Additional arguments that may be used by child implementations.
            **kwargs: **dict
                Additional arguments that may be used by child implementations.
        """

        self.path = Path(path)

    def exists(self) -> bool:
        """
        This function tests the existence of the node data saved to `path`. This method is
        abstract even though this library is ostensibly aimed towards filesystem serialisation.
        Abstraction may allow for more specialised serialisation if needed, for example derived
        implementations of this function could execute database queries.

        Returns
        -------
            This function returns `True` if the node data exists, and `False` otherwise.

        Raises
        ------
            NotImplementedError if the function is not over-written.
            Other exceptions from inherited implementations.
        """

        raise NotImplementedError

    def load(self) -> Any:
        """
        If the node data identified by path exists, its contents loaded and returned by
        this function.

        Returns
        -------
            The contents of the node data. The type is not specified, but should be tested
            and verified by child classes.

        Raises
        ------
            FileNotFoundError: if the file does not exist.
            NotImplementedError: if the function is not over-written.
            Other exceptions from inherited implementations.
        """

        raise NotImplementedError

    def save(self, data: Any) -> None:
        """
        The contents of `data` are saved to the identifier at `path`.

        Parameters
        ----------
            data: The data to be saved; type not specified

        Returns
        -------
            None

        Raises:
            NotImplementedError if the function is not over-written.
            Other exceptions from inherited implementations.
        """

        raise NotImplementedError

    def delete(self) -> None:
        """
        Deletes the contents of `path`

        Returns
        -------
            None

        Raises
        ------
            NotImplementedError if the function is not over-written.
            Other exceptions from inherited implementations.
        """

        raise NotImplementedError

    def lock(self) -> FileLock:
        """
        Returns a `FileLock` object that can be handled with Python's `with` statement.

        Returns
        -------
            FileLock:
                If not used in conjunction of a `with` statement, the behaviour can be managed
                by manually calling `lock.acquire()` and `lock.release()`. Preferred usage is
                the `with` statement.
        """

        self.prepare()

        return FileLock(self.path)

    def prepare(self) -> None:
        """
        Ensures that the path exists before saving

        Returns
        -------
        None
        """

        self.path.parent.mkdir(parents=True, exist_ok=True)


class Backend(object):
    """
    This class manages the serialisation, de-serialisation, deletion, and locking of node data.
    """

    def __init__(self, interface: Type[BackendInterface], cache_data: bool = True):
        """
        Instantiates the backend.

        Parameters
        ----------
            interface: BackendInterface
                The interface of the backend
            cache_data: bool
                If `True`, the result of the evaluation is cached locally in memory. Otherwise the
                resultant computation is not.
        """

        self.cache_data = cache_data
        self.interface = interface

    def get(self, name: str, *args, **kwargs) -> BackendInterface:
        """
        Parameters
        ----------
            name: str
                The unique name of the node.
            *args: Additional arguments for the Interface object.
            **kwargs: Additional keywords for the Interface object.

        Returns
        -------
        BackendInterface
        """

        return self.interface(name, *args, **kwargs)
