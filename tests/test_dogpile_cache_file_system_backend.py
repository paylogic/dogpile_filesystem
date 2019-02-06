import pytest
import dogpile.cache


@pytest.fixture
def region(tmpdir):
    r = dogpile.cache.make_region('test_region')
    r.configure(
        backend='file_system_backend',
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
