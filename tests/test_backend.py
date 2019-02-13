import datetime
import io
import os
import tempfile

import pytest
from dogpile.cache.api import NO_VALUE


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


@pytest.mark.parametrize('backend_name', ['paylogic.raw_fs_backend'])
@pytest.mark.parametrize('backend_file_movable', [False])
@pytest.mark.parametrize('file_creator', [
    pytest.param(lambda _: tempfile.NamedTemporaryFile(delete=False), id='tempfile.NamedTemporaryFile'),
    pytest.param(lambda _: tempfile.NamedTemporaryFile(delete=True), id='tempfile.NamedTemporaryFile(delete)'),
    pytest.param(lambda tmpdir: open(str(tmpdir / 'foo'), 'w+b'), id='open'),
    pytest.param(lambda tmpdir: io.open(str(tmpdir / 'bar'), 'w+b'), id='io.open'),
    pytest.param(lambda tmpdir: (tmpdir / 'baz').open('w+b'), id='tmpdir.open'),
    pytest.param(lambda tmpdir: os.fdopen(os.open(str(tmpdir / 'asd'), os.O_RDWR|os.O_CREAT), 'w+b'), id='os.fdopen'),
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


@pytest.mark.parametrize('backend_name', ['paylogic.raw_fs_backend'])
@pytest.mark.parametrize('backend_file_movable', [True])
@pytest.mark.parametrize('file_creator', [
    pytest.param(lambda _: tempfile.NamedTemporaryFile(delete=False), id='tempfile.NamedTemporaryFile'),
    pytest.param(lambda tmpdir: open(str(tmpdir / 'foo'), 'w+b'), id='open'),
    pytest.param(lambda tmpdir: io.open(str(tmpdir / 'foo'), 'w+b'), id='io.open'),
    pytest.param(lambda tmpdir: (tmpdir / 'foo').open('w+b'), id='tmpdir.open'),
    pytest.param(lambda tmpdir: os.fdopen(os.open(str(tmpdir / 'foo'), os.O_RDWR|os.O_CREAT), 'w+b'), id='os.fdopen'),
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


@pytest.mark.parametrize('backend_cache_size', [None, 30000])
def test_no_cleanup(region):
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
        return arg

    fn(1)
    region.backend.expiration_time = datetime.timedelta(0)

    region.backend.prune()
    assert fn.get(1) is NO_VALUE
