import logging

from .locking import RangedFileReentrantLock
from .utils import ProcessLocalRegistry

logger = logging.getLogger(__name__)

_registry = {}


def _open_file(path):
    """Open a file and keep a reference to it, in order to avoid ResourceWarnings when the interpreter
    closes the file automatically when garbage collected."""
    try:
        return _registry[path]
    except KeyError:
        f = _registry[path] = open(path, "w+b", buffering=0)
        return f


file_lock = ProcessLocalRegistry(creator=_open_file)


def _lock_creator(identifier):
    path, offset = identifier
    file_o = file_lock.get(path)
    return RangedFileReentrantLock(file_o, offset)


locks = ProcessLocalRegistry(creator=_lock_creator)
