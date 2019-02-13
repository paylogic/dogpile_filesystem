import multiprocessing
import threading

import pytest

from dogpile_cache_fs_backend import registry


def test_dogpile_lock_threaded(region):
    mutex = region.backend.get_mutex('asd')

    mutex.acquire()
    mutex.acquire()

    mutex.release()

    thread_result = []

    def other_thread():
        o_mutex = region.backend.get_mutex('asd')
        assert o_mutex is mutex  # TODO: This should be a different test
        acquired = o_mutex.acquire(False)
        if acquired:
            o_mutex.release()
        thread_result.append(acquired)

    t = threading.Thread(target=other_thread)
    t.start()
    t.join()

    try:
        [other_thread_acquired_mutex] = thread_result
        assert other_thread_acquired_mutex is False
    finally:
        mutex.release()


def test_dogpile_lock_processes(region):
    mutex = region.backend.get_mutex('asd')

    mutex.acquire()
    mutex.acquire()

    mutex.release()

    proc_result = multiprocessing.Value('d', 42)
    assert proc_result.value == 42

    def other_process():
        o_mutex = region.backend.get_mutex('asd')
        proc_result.value = o_mutex.acquire(False)

    t = multiprocessing.Process(target=other_process)
    t.start()
    t.join()

    mutex.release()
    assert proc_result.value == 0
    assert not mutex.is_locked()


def test_locks_are_released_on_dereference(region):
    mutex = region.backend.get_mutex('asd')
    mutex.acquire()
    del mutex

    mutex = region.backend.get_mutex('asd')
    assert not mutex.is_locked()
    del mutex
    pass


@pytest.mark.parametrize('n_locks', [1, 2, 1000])
@pytest.mark.parametrize('n_files', [1, 2, 10])
def test_can_acquire_n_locks(tmpdir, n_locks, n_files):
    lockset = []
    for file_i in range(n_files):
        lock_file_path = str(tmpdir / 'lock_' + file_i)
        for i in range(n_locks):
            lock = registry.locks.get((lock_file_path, i))
            lock.acquire()
            lockset += [lock]
    for lock in lockset:
        lock.release()
