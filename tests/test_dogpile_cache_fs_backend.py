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


def test_dogpile_lock(region):
    mutex = region.backend.get_mutex('asd')

    mutex.acquire()
    mutex.acquire()

    mutex.release()

    thread_result = []
    def other_thread():
        thread_result.append(mutex.acquire(blocking=False))

    t = threading.Thread(target=other_thread)
    t.start()
    t.join()

    [other_thread_acquired_mutex] = thread_result

    try:
        assert other_thread_acquired_mutex is False
    finally:
        mutex.release()
