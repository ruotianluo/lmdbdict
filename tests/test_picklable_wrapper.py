import pytest
from lmdbdict.utils import PicklableWrapper, picklable_wrapper
import pickle
try:
    import cloudpickle
except:
    CLOUDPICKLE_AVAILABLE = False
else:
    CLOUDPICKLE_AVAILABLE = True
from unittest import mock


@pytest.mark.skipif(not CLOUDPICKLE_AVAILABLE, reason="PickableWrapper requires cloudpickle")
def test_picklablewrapper():
    fn_bytes = pickle.dumps(PicklableWrapper(lambda x: x))
    fn = pickle.loads(fn_bytes)

    assert fn(1) == 1


def test_picklable_wrapper():
    # If object is pickable, it should remain the same after called picklable_wrapper
    tmp = 1
    assert pickle.dumps(tmp) == pickle.dumps(picklable_wrapper(tmp))

def _temporary_func():
    return 1


@pytest.mark.skipif(not CLOUDPICKLE_AVAILABLE, reason="PickableWrapper requires cloudpickle")
def test_no_cloudpickle_when_receive():
    # If object is pickable, it should remain the same after called picklable_wrapper

    # with cloudpickle
    tmp = lambda x: x
    tmp = pickle.dumps(PicklableWrapper(tmp))
    with mock.patch('lmdbdict.utils.CLOUDPICKLE_AVAILABLE', False):
        with mock.patch('lmdbdict.utils.cloudpickle', pickle):
            # without cloudpickle
            with pytest.raises(ValueError):
                pickle.loads(tmp)

    
    # with cloudpickle
    tmp = pickle.dumps(PicklableWrapper(_temporary_func))
    with mock.patch('lmdbdict.utils.CLOUDPICKLE_AVAILABLE', False):
        with mock.patch('lmdbdict.utils.cloudpickle', pickle):
            # without cloudpickle
            pickle.loads(tmp)