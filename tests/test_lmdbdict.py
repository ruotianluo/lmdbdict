from lmdbdict import lmdbdict, LMDBDict
import pytest
import os
import numpy as np
import pickle
import random
try:
    import cloudpickle
except:
    CLOUDPICKLE_AVAILABLE = False
else:
    CLOUDPICKLE_AVAILABLE = True


@pytest.fixture
def random_input():
    # Various type of inputs
    # int and str inputs
    candidates = list(range(10)) + \
                 list(map(str, range(10)))
    # np inputs
    np_candidates = list(map(np.array, range(10)))  # np array can't be keys
    # return a dict
    d = {}
    for i in range(20):
        d[random.choice(candidates)] = random.choice(candidates+np_candidates)
    return d


@pytest.fixture
def random_lmdbdict(tmpdir, random_input):
    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w')
    for k, v in random_input.items():
        test_dict[k] = v
    del test_dict
    return lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'r')


# Make sure both LMDBDict and lmdbdict works
@pytest.mark.parametrize("module", [
    lmdbdict, LMDBDict
])
def test_basic_functions(tmpdir, random_input, module):
    test_dict = module(os.path.join(tmpdir, 'test.lmdb'), 'w')
    for k, v in random_input.items():
        test_dict[k] = v
    del test_dict
    test_dict = module(os.path.join(tmpdir, 'test.lmdb'), 'r')

    # Assert values are correct
    for k, v in random_input.items():
        assert test_dict[k] == v
    # Assert keys are correct
    assert set(test_dict.keys()) == set(random_input.keys())
    # Assert lens are correct
    assert len(test_dict) == len(random_input)


@pytest.mark.parametrize("key_dumps", [
    None, 'identity', 'ascii',
])
@pytest.mark.parametrize("key_loads", [
    None, 'identity', 'ascii',
])
def test_error(tmpdir, key_dumps, key_loads):
    if key_dumps != key_loads:
        with pytest.raises(AssertionError):
            test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w',
                key_dumps=key_dumps, key_loads=key_loads
            )


@pytest.mark.parametrize("method1", [
    None, 'identity', 'ascii',
])
@pytest.mark.parametrize("method2", [
    'identity', 'ascii',
])
def test_dumpsloads_from_nonempty_error(tmpdir, method1, method2):
    with pytest.raises(AssertionError):
        test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w',
            key_dumps=method1, key_loads=method1
        )
        test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w',
            key_dumps=method2, key_loads=method2
        )


@pytest.mark.parametrize("key_method, value_method, inputs", [
    (None, None, ('key', 0)),
    ('identity', 'identity', (b'key', b'value')),
    ('ascii', 'ascii', ('key', 'value')),
    ('utf8', 'utf8', ('健', '值')),
    ('ascii', 'pyarrow', ('key', 'value')),
])
def test_dumps_loads(tmpdir, key_method, value_method, inputs):
    kwargs = dict(
        key_dumps=key_method,
        key_loads=key_method,
        value_dumps=value_method,
        value_loads=value_method
    )
    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w', **kwargs)
    test_dict[inputs[0]] = inputs[1]
    del test_dict

    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'r')
    assert test_dict[inputs[0]] == inputs[1]

    assert test_dict.db_txn.get(b'__value_dumps__') == pickle.dumps(value_method)
    assert test_dict.db_txn.get(b'__value_loads__') == pickle.dumps(value_method)
    assert test_dict.db_txn.get(b'__key_dumps__') == pickle.dumps(key_method)
    assert test_dict.db_txn.get(b'__key_loads__') == pickle.dumps(key_method)


@pytest.mark.parametrize("key_method, value_method, inputs", [
    (None, None, ('key', 0)),
    ('identity', 'identity', (b'key', b'value')),
    ('ascii', 'ascii', ('key', 'value')),
    ('utf8', 'utf8', ('健', '值')),
    ('ascii', 'pyarrow', ('key', 'value')),
])
def test_method(tmpdir, key_method, value_method, inputs):
    kwargs = dict(
        key_method=key_method,
        value_method=value_method,
    )
    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w', **kwargs)
    test_dict[inputs[0]] = inputs[1]
    del test_dict

    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'r')
    assert test_dict[inputs[0]] == inputs[1]

    assert test_dict.db_txn.get(b'__value_dumps__') == pickle.dumps(value_method)
    assert test_dict.db_txn.get(b'__value_loads__') == pickle.dumps(value_method)
    assert test_dict.db_txn.get(b'__key_dumps__') == pickle.dumps(key_method)
    assert test_dict.db_txn.get(b'__key_loads__') == pickle.dumps(key_method)


@pytest.mark.parametrize("method, dumps, loads", [
    ('ascii', None, 'ascii'),
    ('ascii', 'ascii', None),
    ('ascii', 'ascii', 'ascii'),
])
def test_method_dumps_loads_conflict(tmpdir, method, dumps, loads):
    inputs = ('key', 'value')
    kwargs = dict(
        key_method=method,
        key_dumps=dumps,
        key_loads=loads,
    )
    with pytest.raises(AssertionError):
        test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w', **kwargs)


@pytest.mark.skipif(not CLOUDPICKLE_AVAILABLE, reason="PickableWrapper requires cloudpickle")
def test_lambda_funcs_as_dumps_loads_input(tmpdir, random_input):
    kwargs = dict(
        value_dumps=lambda x: pickle.dumps(x),
        value_loads=lambda x: pickle.loads(x)
    )
    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'w', **kwargs)
    for k, v in random_input.items():
        test_dict[k] = v
    del test_dict
    test_dict = lmdbdict(os.path.join(tmpdir, 'test.lmdb'), 'r')

    # Assert values are correct
    for k, v in random_input.items():
        assert test_dict[k] == v
    # Assert keys are correct
    assert set(test_dict.keys()) == set(random_input.keys())
    # Assert lens are correct
    assert len(test_dict) == len(random_input)


# Test unsafe getitem and safe getitem.
@pytest.mark.parametrize("module", [
    lmdbdict, LMDBDict
])
def test_unsafe(tmpdir, random_input, module):
    test_dict = module(os.path.join(tmpdir, 'test.lmdb'), 'w')
    for k, v in random_input.items():
        test_dict[k] = v
    del test_dict
    test_dict = module(os.path.join(tmpdir, 'test.lmdb'), 'r')
    test_dict._keys = []
    with pytest.raises(KeyError):
        for k in random_input:
            test_dict[k]
    test_dict = module(os.path.join(tmpdir, 'test.lmdb'), 'r', unsafe=True)
    test_dict._keys = []
    for k,v in random_input.items():
        assert test_dict[k] == v


def test_sequential_iter(random_lmdbdict):
    for k,v in random_lmdbdict.sequential_iter():
        print(k, v)