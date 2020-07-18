# Modified from https://github.com/facebookresearch/detectron2/blob/ef096f9b2fbedca335f7476b715426594673f463/detectron2/utils/serialize.py
import pickle
try:
    import cloudpickle
except:
    CLOUDPICKLE_AVAILABLE = False
else:
    CLOUDPICKLE_AVAILABLE = True


def picklable_wrapper(obj):
    # Wrap the object with PicklableWrapper only if it's not natively picklable
    # Note: it's not intended to be run a lot of times
    try:
        pickle.dumps(obj)
        return obj
    except:
        return PicklableWrapper(obj)


class PicklableWrapper(object):
    """
    Wrap an object to make it more picklable, note that it uses
    heavy weight serialization libraries that are slower than pickle.
    It's best to use it only on closures (which are usually not picklable).
    This is a simplified version of
    https://github.com/joblib/joblib/blob/master/joblib/externals/loky/cloudpickle_wrapper.py
    """

    def __init__(self, obj):
        self._obj = obj

    def __reduce__(self):
        s = cloudpickle.dumps(self._obj)
        return cloudpickle.loads, (s,)

    def __call__(self, *args, **kwargs):
        return self._obj(*args, **kwargs)

    def __getattr__(self, attr):
        # Ensure that the wrapped object can be used seamlessly as the previous object.
        if attr not in ["_obj"]:
            return getattr(self._obj, attr)
        return getattr(self, attr)