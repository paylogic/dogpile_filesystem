import datetime
import io
import os
import tempfile
import time

import mock as mock
import pytest
from dogpile.cache.api import NO_VALUE

from dogpile_filesystem import backend


def test_normal_usage(region):
    side_effect = []

    @region.cache_on_arguments()
    def fn(arg):
        side_effect.append(arg)
        return arg + 1

    assert fn(1) == 2
    assert fn(1) == 2

    assert side_effect == [1]


@pytest.mark.parametrize('backend_distributed_lock', [True, False, None])
def test_distributed_lock_param(region, backend_distributed_lock):
    side_effect = []

    @region.cache_on_arguments()
    def fn(arg):
        side_effect.append(arg)
        return arg + 1

    if backend_distributed_lock:
        assert region.backend.get_mutex('foo') is not None
    else:
        assert region.backend.get_mutex('foo') is None
    assert fn(1) == 2

    assert side_effect == [1]


def test_delete(region):
    side_effects = []

    @region.cache_on_arguments()
    def fn(arg):
        side_effects.append(arg)
        return arg + 1

    assert fn(1) == 2
    fn.invalidate(1)
    assert fn(1) == 2
    assert side_effects == [1, 1]


@pytest.mark.parametrize('backend_distributed_lock', [
    True,
    pytest.param(False, marks=pytest.mark.skip(reason='Recursive usage with dogpile thread lock is broken')),
    pytest.param(None, marks=pytest.mark.skip(reason='Recursive usage with dogpile thread lock is broken'))
])
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


def test_get_set_multi(region):
    side_effects = []

    @region.cache_multi_on_arguments()
    def fn(*args):
        side_effects.append(args)
        return [arg + 1 for arg in args]

    assert fn(1, 2, 3) == [2, 3, 4]
    assert fn(1, 2, 3) == [2, 3, 4]
    assert side_effects == [(1, 2, 3)]


def test_delete_multi(region):
    side_effects = []

    @region.cache_multi_on_arguments()
    def fn(*args):
        side_effects.append(args)
        return [arg + 1 for arg in args]

    assert fn(1, 2, 3) == [2, 3, 4]
    fn.invalidate(1, 2, 3)
    assert fn(1, 2, 3) == [2, 3, 4]
    assert side_effects == [(1, 2, 3), (1, 2, 3)]


@pytest.mark.parametrize('backend_name', ['paylogic.raw_filesystem'])
@pytest.mark.parametrize('backend_file_movable', [False])
@pytest.mark.parametrize('file_creator', [
    pytest.param(lambda _: tempfile.NamedTemporaryFile(delete=False), id='tempfile.NamedTemporaryFile'),
    pytest.param(lambda _: tempfile.NamedTemporaryFile(delete=True), id='tempfile.NamedTemporaryFile(delete)'),
    pytest.param(lambda tmpdir: open(str(tmpdir / 'foo'), 'w+b'), id='open'),
    pytest.param(lambda tmpdir: io.open(str(tmpdir / 'bar'), 'w+b'), id='io.open'),
    pytest.param(lambda tmpdir: (tmpdir / 'baz').open('w+b'), id='tmpdir.open'),
    pytest.param(lambda tmpdir: os.fdopen(os.open(str(tmpdir / 'asd'), os.O_RDWR | os.O_CREAT), 'w+b'), id='os.fdopen'),
])
def test_file_not_movable_usage(region, tmpdir, file_creator):
    side_effects = []

    @region.cache_on_arguments()
    def fn(arg, size):
        side_effects.append(arg)
        f = file_creator(tmpdir)
        f.write(b'1' * size)
        f.flush()
        f.seek(1)
        return f

    side_effects = []
    with fn('foo', 2) as result:
        assert result.read() == b'1' * 1
    with fn('foo', 2) as result:
        assert result.read() == b'1' * 1
    assert side_effects == ['foo']


@pytest.mark.parametrize('backend_name', ['paylogic.raw_filesystem'])
@pytest.mark.parametrize('backend_file_movable', [True])
@pytest.mark.parametrize('file_creator', [
    pytest.param(lambda _: tempfile.NamedTemporaryFile(delete=False), id='tempfile.NamedTemporaryFile'),
    pytest.param(lambda tmpdir: open(str(tmpdir / 'foo'), 'w+b'), id='open'),
    pytest.param(lambda tmpdir: io.open(str(tmpdir / 'foo'), 'w+b'), id='io.open'),
    pytest.param(lambda tmpdir: (tmpdir / 'foo').open('w+b'), id='tmpdir.open'),
])
def test_file_movable_usage(region, tmpdir, file_creator):
    side_effects = []

    @region.cache_on_arguments()
    def fn(arg, size):
        side_effects.append(arg)
        f = file_creator(tmpdir)
        f.write(b'1' * size)
        f.flush()
        f.seek(1)
        return f

    with fn('foo', 2) as result:
        assert result.read() == b'1' * 1
    with fn('foo', 2) as result:
        assert result.read() == b'1' * 1
    assert side_effects == ['foo']


@pytest.mark.parametrize('backend_cache_size', [15000])
def test_cleanup_when_size_exceeded(region):
    EPSILON = 2000

    @region.cache_on_arguments()
    def fn(arg, size):
        return b'0' * size

    fn('foo', 10000)
    fn('bar', 10000)
    # backend.prune is invoked at the beginning of .set, so we need to invoke it
    # with a fake value to trigger .prune
    fn('baz', 1)

    size = sum(
        os.path.getsize(os.path.join(region.backend.values_dir, f))
        for f in os.listdir(region.backend.values_dir)
        if os.path.isfile(os.path.join(region.backend.values_dir, f))
    )
    assert 10000 < size < 10000 + EPSILON


def test_cleanup_expired_keys(region):
    @region.cache_on_arguments()
    def fn(arg):
        return arg + 1

    assert fn(1) == 2
    assert fn.get(1) == 2

    region.backend.expiration_time = datetime.timedelta(seconds=0)
    region.backend.prune()
    assert fn.get(1) is NO_VALUE
    assert not os.listdir(region.backend.values_dir)
    assert fn(1) == 2


@pytest.mark.parametrize('backend_cache_size', [None, 30000])
def test_no_cleanup_required(region):
    @region.cache_on_arguments()
    def fn(arg, size):
        return b'0' * size

    fn('foo', 10000)
    fn('bar', 10000)
    # backend.prune is invoked at the beginning of .set, so we need to invoke it
    # with a fake value to trigger .prune
    fn('baz', 1)

    size = sum(
        os.path.getsize(os.path.join(region.backend.values_dir, f))
        for f in os.listdir(region.backend.values_dir)
        if os.path.isfile(os.path.join(region.backend.values_dir, f))
    )
    assert size >= 10000 + 10000 + 1


@pytest.mark.parametrize('backend_expiration_time', [datetime.timedelta(seconds=30)])
def test_expired_items_are_not_returned(region):
    @region.cache_on_arguments()
    def fn(arg):
        return arg

    fn(1)
    assert fn.get(1) == 1
    region.backend.expiration_time = datetime.timedelta(seconds=0)

    assert fn.get(1) is NO_VALUE


@pytest.mark.parametrize('backend_expiration_time', [datetime.timedelta(seconds=30)])
def test_expired_items_are_deleted(region):
    @region.cache_on_arguments()
    def fn(arg):
        return arg + 1

    assert fn(1) == fn.get(1) == 2

    region.backend.expiration_time = datetime.timedelta(seconds=0)
    region.backend.prune()
    assert fn.get(1) is NO_VALUE
    assert not os.listdir(region.backend.values_dir)
    assert fn(1) == 2


@pytest.mark.parametrize('backend_expiration_time', [datetime.timedelta(seconds=30)])
def test_unexpired_items_are_notdeleted(region):
    @region.cache_on_arguments()
    def fn(arg):
        return arg + 1

    _40_seconds_ago = time.time() - 40
    with mock.patch('time.time', return_value=_40_seconds_ago) as m:
        assert fn(1) == 2
        assert m.called

    assert fn(2) == 3  # The unexpired key

    region.backend.prune()
    assert fn.get(1) is NO_VALUE  # 1 expired
    assert fn.get(2) == 3


@pytest.mark.parametrize('backend_cache_size', [15000])
def test_cleanup_does_not_get_stuck_in_case_files_are_not_deletable(region):
    @region.cache_on_arguments()
    def fn(arg, size):
        return b'0' * size

    with mock.patch.object(backend.RawFSBackend, 'prune', return_value=None, autospec=True) as m:
        assert fn('foo', 10000)
        assert fn('bar', 10000)
        assert fn('baz', 10000)
        assert m.called

    with mock.patch('dogpile_filesystem.utils.remove_or_warn', return_value=None) as m:
        region.backend.prune()
        assert m.called
