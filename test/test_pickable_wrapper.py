import pytest
from lmdbdict.utils import PicklableWrapper, picklable_wrapper
import pickle
try:
    import cloudpickle
except:
    CLOUDPICKLE_AVAILABLE = False
else:
    CLOUDPICKLE_AVAILABLE = True


@pytest.mark.skipif(not CLOUDPICKLE_AVAILABLE, reason="PickableWrapper requires cloudpickle")
def test_picklablewrapper():
    fn_bytes = pickle.dumps(PicklableWrapper(lambda x: x))
    fn = pickle.loads(fn_bytes)

    assert fn(1) == 1

def test_picklable_wrapper():
    # If object is pickable, it should remain the same after called picklable_wrapper
    tmp = 1
    assert pickle.dumps(tmp) == pickle.dumps(picklable_wrapper(tmp))