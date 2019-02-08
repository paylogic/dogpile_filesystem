import multiprocessing
import threading

import pytest
import dogpile.cache


@pytest.fixture
def region(tmpdir):
    r = dogpile.cache.make_region('test_region')
    r.configure(
        backend='paylogic.fs_backend',
        arguments={
            'base_dir': str(tmpdir),
        },
    )
    return r


def test_normal_usage(region):
    side_effect = []

    @region.cache_on_arguments()
    def fn(arg):
        side_effect.append(arg)
        return arg + 1

    assert fn(1) == 2
    assert fn(1) == 2

    assert side_effect == [1]

def test_recursive_usage(region):
    context = {'value': 3}

    @region.cache_on_arguments()
    def fn():
        if context['value'] == 0:
            return 42
        context['value'] -= 1
        return fn()

    assert fn() == 42
    assert context['value'] == 0



def test_dogpile_lock_threaded(region):
    mutex = region.backend.get_mutex('asd')

    mutex.acquire()
    mutex.acquire()

    mutex.release()

    thread_result = []
    def other_thread():
        o_mutex = region.backend.get_mutex('asd')
        assert o_mutex is mutex
        thread_result.append(o_mutex.acquire(False))

    t = threading.Thread(target=other_thread)
    t.start()
    t.join()

    [other_thread_acquired_mutex] = thread_result

    try:
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

    try:
        assert proc_result.value == 0
    finally:
        mutex.release()
