import lmdb
import pickle
import os


def identity(x):
    return x


VALUE_DUMPS = dict(
    identity=identity
)
VALUE_LOADS = dict(
    identity=identity
)


class LMDBDict:
    def __init__(self, lmdb_path, mode='r',
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
        else: # no keys
            self._keys = set()
            if self.mode == 'r':
                print('Reading an empty lmdb')
        
        if self.db_txn.get(b'__value_dumps__') is not None and\
           self.db_txn.get(b'__value_loads__') is not None:
            saved_dumps = pickle.loads(self.db_txn.get(b'__value_dumps__'))
            saved_loads = pickle.loads(self.db_txn.get(b'__value_loads__'))
            assert (value_dumps == saved_dumps or value_dumps is None) \
                and (value_loads == saved_loads or value_loads is None), \
                'value_dumps and value_loads has to be the same as what\'s saved in the lmdb. Or just feed None'
            value_dumps, value_loads = saved_dumps, saved_loads
        elif self.mode == 'w':
            # Write to the db_txn
            self.db_txn.put(b'__value_dumps__', pickle.dumps(value_dumps))
            self.db_txn.put(b'__value_loads__', pickle.dumps(value_loads))
            self.db_txn.commit()
            self.db_txn = self.env.begin(write=True)
        elif self.mode == 'r':
            # Note, here there is no value dumps and loads in db
            # Will use default pickle
            assert value_dumps is None and value_loads is None, \
                'cannot set the value_dumps and value_loads under read mode'
            print("No value dumps and loads found in lmdb, will use pickle")

        if value_dumps is None and value_loads  is None:
            self._value_dumps = pickle.dumps
            self._value_loads = pickle.loads
        elif type(value_dumps) is str and type(value_loads) is str:
            self._value_dumps = VALUE_DUMPS[value_dumps]
            self._value_loads = VALUE_LOADS[value_loads]
        else: # have to be function
            self._value_dumps = value_dumps
            self._value_loads = value_loads


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
            self.env= lmdb.open(
                self.lmdb_path, subdir=False,
                readonly=False, map_size=1099511627776 * 2,
                meminit=False, map_async=True)
            self.db_txn = self.env.begin(write=True)

    def __getitem__(self, key):
        if key not in self:
            raise KeyError
        return self._value_loads(self.db_txn.get(pickle.dumps(key)))

    def __setitem__(self, key, value):
        assert self.mode == 'w', 'can only write item in write mode'
        # in fact even key is __len__ it should be fine, because it's dumped in pickle mode.
        assert key not in ['__keys__'], f'{key} is internal variable, immutable to users'
        self.db_txn.put(pickle.dumps(key), self._value_dumps(value))
        self._keys.add(key) # only update to the lmdb after flush

    def __delitem__(self, key):
        self.db_txn.delete(pickle.dumps(key))
        self._keys.remove(key)

    def values(self):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def update(self, d):
        assert self.mode == 'w'
        for k,v in d.items():
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
