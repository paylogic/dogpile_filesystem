import codecs
import datetime
import errno
import hashlib
import logging
import os
import sys

import pytz
import six

logger = logging.getLogger(__name__)


def _remove(file_path):
    try:
        os.remove(file_path)
    except (IOError, OSError):
        logger.exception('Cannot remove file {}'.format(file_path))


def _ensure_dir(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise
    except (OSError, IOError):
        logger.exception('Cannot create directory {}'.format(path))


def _stat(file_path):
    try:
        return os.stat(file_path)
    except (IOError, OSError):
        logger.exception('Cannot stat file {}'.format(file_path))
        return None


def _get_size(stat):
    if stat is None:
        return 0
    return stat.st_size


def _get_last_modified(stat):
    if stat is None:
        return datetime.datetime.fromtimestamp(0, tz=pytz.utc)
    return datetime.datetime.fromtimestamp(stat.st_mtime, pytz.utc)


def without_suffixes(string, suffixes):
    for suffix in suffixes:
        if string.endswith(suffix):
            return string[:-len(suffix)]
    return string


def sha256_mangler(key):
    if isinstance(key, six.text_type):
        key = key.encode('utf-8')
    return hashlib.sha256(key).hexdigest()


def _key_to_offset(key, max=sys.maxint):
    # Map any string to randomly distributed integers between 0 and max
    hash = hashlib.sha1(key.encode('utf-8')).digest()
    return int(codecs.encode(hash, 'hex'), 16) % max
