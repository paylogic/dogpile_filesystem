import os

import pytest


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


@pytest.mark.parametrize('region_cache_size', [15000])
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


