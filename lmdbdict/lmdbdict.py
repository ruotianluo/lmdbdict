import lmdb
import pickle
import os

try:
    import pyarrow as pa
except ImportError:
    PYARROW_AVAILABLE = False
else:
    PYARROW_AVAILABLE = True


# Only use when you are sure the input is a byte
def identity(x):
    return x


def ascii_encode(x):
    return x.encode('ascii')


def ascii_decode(x):
    return x.decode('ascii')


def pa_dumps(x):
    assert PYARROW_AVAILABLE, 'pyarrow not installed'
    return pa.serialize(x).to_buffer()


def pa_loads(x):
    assert PYARROW_AVAILABLE, 'pyarrow not installed'
    return pa.deserialize(x)


DUMPS_FUNC = dict(
    identity=identity,
    ascii=ascii_encode,
    pyarrow=pa_dumps,
)

LOADS_FUNC = dict(
    identity=identity,
    ascii=ascii_decode,
    pyarrow=pa_loads,
)


class LMDBDict:
    def __init__(self, lmdb_path, mode='r',
                 key_dumps=None, key_loads=None,
                 value_dumps=None, value_loads=None):
        """
        value_dumps/loads can be picklable functions
        or str or None
        if None: then default pickle
        if 'identity' then func = lambda x: x
        if saved in the db, then use what's in db
        """
        self.lmdb_path = lmdb_path
        self.mode = mode
        self._init_db()
        if self.db_txn.get(b'__keys__'):
            self._keys = pickle.loads(self.db_txn.get(b'__keys__'))
        else:  # no keys
            self._keys = set()
            if self.mode == 'r':
                print('Reading an empty lmdb')

        self._init_dumps_loads(value_dumps, value_loads, which='value')
        self._init_dumps_loads(key_dumps, key_loads, which='key')

    def _init_dumps_loads(self, dumps, loads, which='value'):
        """
        Initialize the key/value dumps loads function according to
        the user input or the db.
        """
        # The keys in the db
        db_dumps = f'__{which}_dumps__'.encode('ascii')
        db_loads = f'__{which}_loads__'.encode('ascii')

        if self.db_txn.get(db_dumps) is not None and\
           self.db_txn.get(db_loads) is not None:
            saved_dumps = pickle.loads(self.db_txn.get(db_dumps))
            saved_loads = pickle.loads(self.db_txn.get(db_loads))
            assert (dumps == dumps or dumps is None) \
                and (loads == loads or loads is None), \
                f'{which}_dumps and {which}_loads has to be the same as what\'s saved in the lmdb. Or just feed None'
            dumps, loads = saved_dumps, saved_loads
        elif self.mode == 'w':
            # Write to the db_txn
            self.db_txn.put(db_dumps, pickle.dumps(dumps))
            self.db_txn.put(db_loads, pickle.dumps(loads))
            self.db_txn.commit()
            self.db_txn = self.env.begin(write=True)
        elif self.mode == 'r':
            # Note, here there is no value dumps and loads in db
            # Will use default pickle
            assert dumps is None and loads is None, \
                f'cannot set the {which}_dumps and {which}_loads under read mode'
            print("No {which} dumps and loads found in lmdb, will use pickle")

        if dumps is None or loads is None:
            assert dumps == loads, f'The {which}_dumps and {which}_loads have to be both None'
            setattr(self, f'_{which}_dumps', pickle.dumps)
            setattr(self, f'_{which}_loads', pickle.loads)
        elif type(dumps) is str and type(loads) is str:
            assert dumps == loads, f'The {which}_dumps and {which}_loads have to correspondant'
            setattr(self, f'_{which}_dumps', DUMPS_FUNC[dumps])
            setattr(self, f'_{which}_loads', LOADS_FUNC[loads])
        else:  # have to be function
            setattr(self, f'_{which}_dumps', dumps)
            setattr(self, f'_{which}_loads', loads)

    def keys(self):
        return list(self._keys)

    def __contains__(self, item):
        return item in self._keys

    def __getstate__(self):
        r"""
        Make it pickable
        """
        state = self.__dict__
        state["env"] = None
        state["db_txn"] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._init_db()

    def _init_db(self):
        if self.mode == 'r':
            self.env = lmdb.open(
                self.lmdb_path,
                subdir=os.path.isdir(self.lmdb_path),
                readonly=True, lock=False,
                readahead=False, map_size=1099511627776 * 2,
                max_readers=100,
            )
            self.db_txn = self.env.begin(write=False)
        elif self.mode == 'w':
            self.env = lmdb.open(
                self.lmdb_path, subdir=False,
                readonly=False, map_size=1099511627776 * 2,
                meminit=False, map_async=True)
            self.db_txn = self.env.begin(write=True)

    def __getitem__(self, key):
        if key not in self:
            raise KeyError
        return self._value_loads(self.db_txn.get(self._key_dumps(key)))

    def __setitem__(self, key, value):
        assert self.mode == 'w', 'can only write item in write mode'
        # in fact even key is __len__ it should be fine, because it's dumped in pickle mode.
        assert key not in ['__keys__'], \
            f'{key} is internal variable, immutable to users'
        self.db_txn.put(self._key_dumps(key), self._value_dumps(value))
        self._keys.add(key)  # only update to the lmdb after flush

    def __delitem__(self, key):
        self.db_txn.delete(self._key_dumps(key))
        self._keys.remove(key)

    def values(self):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def update(self, d):
        assert self.mode == 'w'
        for k, v in d.items():
            self[k] = v

    def __len__(self):
        return len(self._keys)

    def __repr__(self):
        return self.__class__.__name__ + ' (' + self.lmdb_path + ')'

    def __del__(self):
        # make sure the the __key__ and __len__ are updated
        # and it's flushed
        if self.mode == 'w':
            self.flush()
            self.env.sync()
            self.env.close()

    def flush(self):
        assert self.mode == 'w', 'only flush when in write mode'
        # update __keys__ value
        self.db_txn.put(b'__keys__', pickle.dumps(self._keys))
        self.db_txn.commit()
        self.db_txn = self.env.begin(write=True)
