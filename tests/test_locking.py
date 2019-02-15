import errno
import multiprocessing
import threading

import pytest

from dogpile_filesystem import registry


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


# TODO: Refactor this mess
def _test_avoid_deadlock_process(queue, result_q):
    from dogpile_filesystem import registry

    locks = []
    while True:
        item = queue.get()
        if item == 'done':
            break
        lock = registry.locks.get(item)
        try:
            lock.acquire()
        except Exception as e:
            result_q.put(e)
            return
        finally:
            queue.task_done()
        locks.append(lock)

    for lock in locks:
        lock.release()
    queue.task_done()
    result_q.put('success')


def test_avoid_deadlock(tmpdir):
    lock_file_path = str(tmpdir / 'lock')

    q1 = multiprocessing.JoinableQueue()
    q1result = multiprocessing.JoinableQueue()
    q2 = multiprocessing.JoinableQueue()
    q2result = multiprocessing.JoinableQueue()
    process1 = multiprocessing.Process(target=_test_avoid_deadlock_process, args=[q1, q1result])
    process2 = multiprocessing.Process(target=_test_avoid_deadlock_process, args=[q2, q2result])
    process1.start()
    process2.start()

    q1.put((lock_file_path, 1))
    q1.join()  # the other process acquired mutex 1

    q2.put((lock_file_path, 2))
    q2.join()  # the other process acquired mutex 1

    q1.put((lock_file_path, 2))
    q2.put((lock_file_path, 1))
    q1.join()
    q2.join()
    q1.put('done')
    q2.put('done')

    results = {q1result.get(), q2result.get()}
    process1.join()
    process2.join()
    while results:
        result = results.pop()
        if result == 'success':
            continue
        assert isinstance(result, IOError)
        assert result.errno == errno.EDEADLK
