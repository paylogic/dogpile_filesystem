

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
