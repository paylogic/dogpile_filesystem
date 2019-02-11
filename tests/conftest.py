import dogpile.cache
import pytest


@pytest.fixture
def region_base_dir(tmpdir):
    return str(tmpdir)


@pytest.fixture
def region_cache_size():
    return None


@pytest.fixture
def region(region_base_dir, region_cache_size):
    r = dogpile.cache.make_region('test_region')
    r.configure(
        backend='paylogic.fs_backend',
        arguments={
            'base_dir': region_base_dir,
            'cache_size': region_cache_size,
        },
    )
    return r
