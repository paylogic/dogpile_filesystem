import logging

from .locking import RangedFileReentrantLock
from .utils import ProcessLocalRegistry

logger = logging.getLogger(__name__)

file_lock = ProcessLocalRegistry(lambda path: open(path, 'w+b'))


def _lock_creator(identifier):
    path, offset = identifier
    file_o = file_lock.get(path)
    return RangedFileReentrantLock(file_o, offset)


locks = ProcessLocalRegistry(_lock_creator)
