import dogpile.cache
import pytest


@pytest.fixture
def backend_base_dir(tmpdir):
    return str(tmpdir)


@pytest.fixture
def backend_cache_size():
    return None


@pytest.fixture
def backend_expiration_time():
    return None


@pytest.fixture
def backend_file_movable():
    return False


@pytest.fixture
def backend_arguments(
    backend_base_dir,
    backend_cache_size,
    backend_expiration_time,
    backend_file_movable,
):
    return {
        'base_dir': backend_base_dir,
        'cache_size': backend_cache_size,
        'expiration_time': backend_expiration_time,
        'file_movable': backend_file_movable,
    }


@pytest.fixture
def region_expiration_time():
    return None


@pytest.fixture
def region(backend_arguments, region_expiration_time):
    r = dogpile.cache.make_region('test_region')
    r.configure(
        backend='paylogic.fs_backend',
        expiration_time=region_expiration_time,
        arguments=backend_arguments,
    )
    return r
