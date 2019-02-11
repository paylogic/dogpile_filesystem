"""
File Backends
------------------

Provides backends that deal with local filesystem access.

"""
import datetime
import hashlib
import io
import os
import pickle
import tempfile

from shutil import copyfileobj

import pytz  # TODO: Remove this dependency

from dogpile.cache.api import CacheBackend, NO_VALUE, CachedValue

from . import registry
from .utils import _remove, _ensure_dir, _stat, _get_size, _get_last_modified, without_suffixes, _key_to_offset

__all__ = ['FSBackend']


class FSBackend(CacheBackend):
    """A file-backend using files to store keys.

    Basic usage::

        from dogpile.cache import make_region

        region = make_region().configure(
            'paylogic.files_backend',
            expiration_time = datetime.timedelta(seconds=30),
            arguments = {
                "base_dir": "/path/to/cachedir",
                "file_movable": True,
                "cache_size": 1024*1024*1024,  # 1 Gb
                "expiration_time: datetime.timedelta(seconds=30),
            }
        )

        @region.cache_on_arguments()
        def big_file_operation(args):
            f = tempfile.NamedTemporaryFile(delete=False)
            # fill the file
            f.flush()
            f.seek(0)
            return f


    Parameters to the ``arguments`` dictionary are
    below.

    :param base_dir: path of the directory where to store the files.
    :param expiration_time: expiration time of the keys
    :param cache_size: the maximum size of the directory. Once exceeded, keys will be removed in a
                       LRU fashion.
    :param file_movable: whether the file provided to .set() can be moved. If that's the case,
                         the backend will avoid copying the contents.
    """

    @staticmethod
    def key_mangler(key):
        return hashlib.sha256(key.encode('utf-8')).hexdigest()

    def __init__(self, arguments):
        # TODO: Add self.lock_expiration
        self.base_dir = os.path.abspath(
            os.path.normpath(arguments['base_dir'])
        )
        _ensure_dir(self.base_dir)

        self.values_dir = os.path.join(self.base_dir, 'values')
        _ensure_dir(self.values_dir)

        self.dogpile_lock_path = os.path.join(self.base_dir, 'dogpile.lock')
        self.rw_lock_path = os.path.join(self.base_dir, 'rw.lock')

        self.expiration_time = arguments.get('expiration_time')
        self.cache_size = arguments.get('cache_size', 1024 * 1024 * 1024)  # 1 Gb
        self.file_movable = arguments.get('file_movable', False)
        self.distributed_lock = arguments.get('distributed_lock', True)

    def _get_rw_lock(self, key):
        identifier = (self.rw_lock_path, _key_to_offset(key))
        return registry.locks.get(identifier)

    def _get_dogpile_lock(self, key):
        identifier = (self.dogpile_lock_path, _key_to_offset(key))
        return registry.locks.get(identifier)

    def get_mutex(self, key):
        if self.distributed_lock:
            return self._get_dogpile_lock(key)

            # # We need to hash the key, as it may not have used our key mangler
            # hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
            # partition_key = hash[:self.dogpile_lock_partition_size * 2]
            # return FileLock(os.path.join(self.base_dir, 'dogpile_locks', partition_key))
        else:
            return None

    def _file_path_payload(self, key):
        return os.path.join(self.values_dir, key + '.payload')

    def _file_path_metadata(self, key):
        return os.path.join(self.values_dir, key + '.metadata')

    def _file_path_type(self, key):
        return os.path.join(self.values_dir, key + '.type')

    def get(self, key):
        now = datetime.datetime.now(tz=pytz.utc)
        file_path_payload = self._file_path_payload(key)
        file_path_metadata = self._file_path_metadata(key)
        file_path_type = self._file_path_type(key)
        with self._get_rw_lock(key):
            if not os.path.exists(file_path_payload) or not os.path.exists(file_path_metadata) \
                or not os.path.exists(file_path_type):
                return NO_VALUE
            if self.expiration_time is not None:
                if _get_last_modified(_stat(file_path_payload)) < now - self.expiration_time:
                    return NO_VALUE

            with open(file_path_metadata, 'rb') as i:
                metadata = pickle.load(i)
            with open(file_path_type, 'rb') as i:
                type = pickle.load(i)
            if type == 'file':
                return CachedValue(
                    open(file_path_payload, 'rb'),
                    metadata,
                )
            elif metadata is not None:
                with open(file_path_payload, 'rb') as i:
                    return CachedValue(
                        pickle.load(i),
                        metadata,
                    )
            else:
                with open(file_path_payload, 'rb') as i:
                    return pickle.load(i)

    def get_multi(self, keys):
        return [self.get(key) for key in keys]

    def set(self, key, value):
        self.prune()
        if isinstance(value, CachedValue):
            payload, metadata = value.payload, value.metadata
        else:
            payload, metadata = value, None
        with tempfile.NamedTemporaryFile(delete=False) as metadata_file:
            pickle.dump(metadata, metadata_file, pickle.HIGHEST_PROTOCOL)

        if not isinstance(payload, io.IOBase):
            type = 'value'
            with tempfile.NamedTemporaryFile(delete=False) as payload_file:
                pickle.dump(payload, payload_file, pickle.HIGHEST_PROTOCOL)
            payload_file_path = payload_file.name
        else:
            type = 'file'
            if self.file_movable and hasattr(payload, 'name'):
                payload_file_path = payload.name
            else:
                payload.seek(0)

                with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                    copyfileobj(payload, tmpfile, length=1024 * 1024)
                # TODO: seek to the original cursor
                payload_file_path = tmpfile.name

        with tempfile.NamedTemporaryFile(delete=False) as type_file:
            pickle.dump(type, type_file, pickle.HIGHEST_PROTOCOL)

        with self._get_rw_lock(key):
            os.rename(metadata_file.name, self._file_path_metadata(key))
            os.rename(type_file.name, self._file_path_type(key))
            os.rename(payload_file_path, self._file_path_payload(key))

    def set_multi(self, mapping):
        for key, value in mapping.items():
            self.set(key, value)

    def delete(self, key):
        with self._get_rw_lock(key):
            self._delete_key_files(key)

    def delete_multi(self, keys):
        for key in keys:
            self.delete(key)

    def _delete_key_files(self, key):
        _remove(self._file_path_payload(key))
        _remove(self._file_path_metadata(key))
        _remove(self._file_path_type(key))

    def _list_keys_with_desc(self):
        suffixes = ['.payload', '.metadata', '.type']
        files = [
            file_name for file_name in os.listdir(self.values_dir)
            if any(file_name.endswith(s) for s in suffixes)
        ]
        files_with_stats = {
            f: _stat(os.path.join(self.values_dir, f)) for f in files
        }

        keys = set(
            without_suffixes(f, suffixes)
            for f in files
        )

        return {
            key: {
                'last_modified': _get_last_modified(files_with_stats.get(key + '.payload', None)),
                'size': (
                    _get_size(files_with_stats.get(key + '.payload'))
                    + _get_size(files_with_stats.get(key + '.metadata'))
                    + _get_size(files_with_stats.get(key + '.type'))
                ),
            }
            for key in keys
        }

    def attempt_delete_key(self, key):
        rw_lock = self._get_rw_lock(key)
        if rw_lock.acquire(blocking=False):
            try:
                self._delete_key_files(key)
            finally:
                rw_lock.release()

    def prune(self):
        now = datetime.datetime.now(tz=pytz.utc)
        keys_with_desc = self._list_keys_with_desc()
        keys = set(keys_with_desc)
        remaining_keys = set(keys_with_desc)

        if self.expiration_time is not None:
            for key in keys:
                if keys_with_desc[key]['last_modified'] >= now - self.expiration_time:
                    continue
                self.attempt_delete_key(key)
                remaining_keys.discard(key)

        keys_by_newest = sorted(
            remaining_keys,
            key=lambda key: keys_with_desc[key]['last_modified'],
            reverse=True,
        )
        while sum((keys_with_desc[key]['size'] for key in keys_by_newest), 0) > self.cache_size:
            if not keys_by_newest:
                break
            key = keys_by_newest.pop()
            self.attempt_delete_key(key)
