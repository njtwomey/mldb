# Modified version of
#   https://github.com/ilastik/lazyflow/blob/master/lazyflow/utility/fileLock.py
# The modifications introduced are to simply raise a FileLockExistsException if the lock file exists
# without attempting to re-lock. Repeated attempts ought to be made by the main file.

import os
import sys

__all__ = [
    'FileLockExistsException', 'FileLock',
]


class FileLockExistsException(Exception):
    pass


class FileLock(object):
    """
    A file locking mechanism that has context-manager support so you can use it in a ``with`` statement.
    This should be relatively cross compatible as it doesn't rely on ``msvcrt`` or ``fcntl`` for the
    locking.
    """
    
    def __init__(self, protected_file_path, lock_file_contents=None):
        """
        Prepare the file locker. Specify the file to lock and optionally the maximum timeout and the
        delay between each attempt to lock.
        """
        self.is_locked = False
        self.lockfile = f"{protected_file_path}.lock"
        
        _lock_file_contents = ["Owning process args:"]
        for arg in sys.argv:
            _lock_file_contents.append(arg)
        _lock_file_contents.append(f'Node name: {lock_file_contents}')
        self._lock_file_contents = '\n'.join(_lock_file_contents)
    
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
        return not os.path.exists(self.lockfile)
    
    def acquire(self, blocking=True):
        """
        Acquire the lock, if possible. If the lock is in use, and `blocking` is False, return False.
        Otherwise, check again every `self.delay` seconds until it either gets the lock or exceeds `timeout`
        number of seconds, in which case it raises an exception.
        """
        # Attempt to create the lockfile.
        # These flags cause os.open to raise an OSError if the file already exists.
        try:
            fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            with os.fdopen(fd, "a") as f:
                # Print some info about the current process as debug info for anyone who bothers to look.
                f.write(self._lock_file_contents)
        except FileExistsError:
            raise FileLockExistsException
        self.is_locked = True
        return True
    
    def release(self):
        """
        Get rid of the lock by deleting the lockfile. When working in a `with` statement, this gets
        automatically called at the end.
        """
        self.is_locked = False
        os.unlink(self.lockfile)
    
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
        if os.path.exists(self.lockfile):
            self.release()
            return True
        return False
