import errno
import logging
import os
import threading

from dogpile.cache import util

logger = logging.getLogger(__name__)


class RangedFileReentrantLock(object):
    def __init__(self, file, offset):
        if file is None:
            raise ValueError('file parameter cannot be None')
        if offset is None:
            raise ValueError('offset parameter cannot be None')
        self._offset = offset
        self._file = file
        self._thread_lock = threading.RLock()
        self._counter = 0
        self._pid = os.getpid()

    def is_locked(self):
        return self._counter > 0

    @util.memoized_property
    def _module(self):
        import fcntl
        return fcntl

    def _assert_pid(self):
        if os.getpid() != self._pid:
            raise RuntimeError('Cannot use this lock, since it was created by a different process.')

    def acquire(self, blocking=True):
        self._assert_pid()
        lockflag = self._module.LOCK_EX
        if not blocking:
            lockflag |= self._module.LOCK_NB
        acquired = self._thread_lock.acquire(blocking)
        if not blocking and not acquired:
            return False

        if self._counter == 0:
            try:
                logger.debug('lockf({self._file.name}, pid={self._pid}, blocking={blocking}, '
                             'offset={self._offset})'.format(self=self, blocking=blocking))
                self._module.lockf(self._file, lockflag, 1, self._offset)
                # if self._offset is not None:
                #     self._module.lockf(self._file, lockflag, 1, self._offset)
                # else:
                #     self._module.lockf(self._file, lockflag)
                logger.debug('!lockf({self._file.name}, pid={self._pid}, blocking={blocking}, '
                             'offset={self._offset})'.format(self=self, blocking=blocking))
            except IOError as e:
                self._thread_lock.release()
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    return False
                raise

        self._counter += 1
        return True

    def release(self):
        self._assert_pid()
        self._counter -= 1
        assert self._counter >= 0
        if self._counter > 0:
            self._thread_lock.release()
            return

        try:
            logger.debug('unlockf({}, offset={})'.format(
                getattr(self._file, 'name', self._file), self._offset
            ))
            self._module.lockf(self._file, self._module.LOCK_UN, 1, self._offset)
            # if self._offset is not None:
            #     self._module.lockf(self._file, self._module.LOCK_UN, 1, self._offset)
            # else:
            #     self._module.lockf(self._file, self._module.LOCK_UN)
        finally:
            # DO NOT assign self._file to None. Otherwise in case this object is not dereferenced, nobody else can do
            # acquire on it after the last release.
            # self._file = None
            self._thread_lock.release()

    def __enter__(self):
        self.acquire(blocking=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
