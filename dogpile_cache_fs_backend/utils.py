import codecs
import errno
import hashlib
import os
import sys
import threading
import warnings

from dogpile.util import NameRegistry


def remove_or_warn(file_path):
    try:
        os.remove(file_path)
    except (IOError, OSError):
        warnings.warn('Cannot remove file {}'.format(file_path))


def ensure_dir(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise


def stat_or_warn(file_path):
    try:
        return os.stat(file_path)
    except (IOError, OSError):
        warnings.warn('Cannot stat file {}'.format(file_path))
        return None


def _get_size(stat):
    if stat is None:
        return 0
    return stat.st_size


def _get_last_modified(stat):
    if stat is None:
        return 0
    return stat.st_mtime


def without_suffixes(string, suffixes):
    for suffix in suffixes:
        if string.endswith(suffix):
            return string[:-len(suffix)]
    return string


def _key_to_offset(key, start=0, end=sys.maxsize):
    # Map any string to randomly distributed integers between 0 and max
    hash_ = hashlib.sha1(key.encode('utf-8')).digest()
    offset_from_0 = (int(codecs.encode(hash_, 'hex'), 16) % (end - start))
    return start + offset_from_0


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
        self._lock = threading.Lock()

    def get(self, identifier, *args, **kwargs):
        current_pid = os.getpid()
        if self._pid != current_pid:
            with self._lock:
                # Let's check it again, another thread may have fixed it before I got the lock
                if self._pid != current_pid:
                    self._pid, self.registry = current_pid, NameRegistry(creator=self.creator)
        return self.registry.get(identifier, *args, **kwargs)
