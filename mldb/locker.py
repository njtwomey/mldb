# Modified version of
#   https://github.com/ilastik/lazyflow/blob/master/lazyflow/utility/fileLock.py
# The modifications introduced are to simply raise a FileLockExistsException if the lock file exists
# without attempting to re-lock and to use pathlib. Repeated attempts ought to be made by the main file.
import fcntl
from pathlib import Path

__all__ = [
    "FileLockExistsException",
    "FileLock",
]


class FileLockExistsException(Exception):
    pass


class FileLock(object):
    """
    A file locking mechanism that has context-manager support so you can use it in a ``with`` statement.
    This should be relatively cross compatible as it doesn't rely on ``msvcrt`` or ``fcntl`` for the
    locking.
    """

    def __init__(self, protected_file_path):
        """
        Prepare the file locker. Specify the file to lock and optionally the maximum timeout and the
        delay between each attempt to lock.
        """
        self.is_locked = False
        self.lock_filename = Path(protected_file_path).with_suffix(".lock")
        self.lock_file = None

    def locked(self):
        """
        Returns True iff the file is owned by THIS FileLock instance. (Even if this returns false, the file
        could be owned by another FileLock instance, possibly in a different thread or process).
        """
        return self.is_locked

    def available(self):
        """
        Returns True iff the file is currently available to be locked.
        """
        return not self.lock_filename.exists()

    def acquire(self, blocking=True):
        """
        Acquire the lock, if possible. If the lock is in use, and `blocking` is False, return False.
        Otherwise, check again every `self.delay` seconds until it either gets the lock or exceeds `timeout`
        number of seconds, in which case it raises an exception.
        """
        # Attempt to create the lockfile.
        # These flags cause os.open to raise an OSError if the file already exists.
        try:
            self.lock_file = open(self.lock_filename, "w")
            fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except FileNotFoundError:
            raise FileNotFoundError
        except IOError:
            raise FileLockExistsException(f"The file {self.lock_filename} is currently in use.")
        self.is_locked = True
        return True

    def release(self):
        """
        Get rid of the lock by deleting the lockfile. When working in a `with` statement, this gets
        automatically called at the end.
        """
        self.is_locked = False
        fcntl.flock(self.lock_file, fcntl.LOCK_UN)
        try:
            self.lock_filename.unlink()
        except FileNotFoundError:
            pass

    def __enter__(self):
        """
        Activated when used in the with statement. Should automatically acquire a lock to be used
        in the with block.
        """
        self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        """
        Activated at the end of the with statement. It automatically releases the lock if it isn't
        locked.
        """
        self.release()

    def __del__(self):
        """
        Make sure this ``FileLock`` instance doesn't leave a .lock file lying around.
        """
        if self.is_locked:
            self.release()

    def purge(self):
        """
        For debug purposes only.  Removes the lock file from the hard disk.
        """
        if self.lock_filename.exists():
            self.release()
            return True
        return False
