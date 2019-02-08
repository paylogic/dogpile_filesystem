import errno
import logging
import os
import threading

from dogpile.cache import util
from dogpile.util import NameRegistry

logger = logging.getLogger(__name__)


class ProcessLocalRegistry(object):
    """
    Provides a basic per-process mapping container that wipes itself if the current PID changed since the last get/set.
    Aka `threading.local()`, but for processes instead of threads.
    """

    def __init__(self, creator):
        super(ProcessLocalRegistry, self).__init__()
        self._pid = None
        self.creator = creator
        self.registry = None

    def get(self, identifier, *args, **kwargs):
        current_pid = os.getpid()
        if self._pid != current_pid:
            self._pid, self.registry = current_pid, NameRegistry(creator=self.creator)
        return self.registry.get(identifier, *args, **kwargs)


class RangedFileReentrantLock(object):
    def __init__(self, file, offset):
        self.key_offset = offset
        self._file = file
        self.lock = threading.RLock()
        self.counter = 0

    @util.memoized_property
    def _module(self):
        import fcntl
        return fcntl

    def acquire(self, blocking=True):
        lockflag = self._module.LOCK_EX
        if not blocking:
            lockflag |= self._module.LOCK_NB
        acquired = self.lock.acquire(blocking)
        if not blocking and not acquired:
            return False

        if self.counter == 0:
            try:
                logger.debug('lockf({}, blocking={}, offset={}'.format(
                    getattr(self._file, 'name', self._file), blocking, self.key_offset
                ))
                if self.key_offset is not None:
                    self._module.lockf(self._file, lockflag, 1, self.key_offset)
                else:
                    self._module.lockf(self._file, lockflag)
                logger.debug('! lockf({}, blocking={}, offset={}'.format(
                    getattr(self._file, 'name', self._file), blocking, self.key_offset
                ))
            except IOError as e:
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    return False
                raise
                # os.close(fileno)
        self.counter += 1
        return True

    def release(self):
        # if self._filedescriptor is None:
        #     return
        self.counter -= 1
        assert self.counter >= 0
        if self.counter > 0:
            self.lock.release()
            return

        try:
            logger.debug('unlockf({}, offset={}'.format(
                getattr(self._file, 'name', self._file), self.key_offset
            ))
            if self.key_offset is not None:
                self._module.lockf(self._file, self._module.LOCK_UN, 1, self.key_offset)
            else:
                self._module.lockf(self._file, self._module.LOCK_UN)
        finally:
            self._file = None
            self.lock.release()

    def __enter__(self):
        self.acquire(blocking=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
