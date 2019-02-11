import dogpile.cache
import pytest


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
