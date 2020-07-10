import lmdb
import pickle
import os

class LMDBDict:
    def __init__(self, lmdb_path, mode='r'):
        self.lmdb_path = lmdb_path
        self.mode = mode
        self._init_db()
        if self.db_txn.get(b'__keys__'):
            self._keys = pickle.loads(self.db_txn.get(b'__keys__'))
        else: # no keys
            self._keys = set()
            if self.mode == 'r':
                print('Reading an empty lmdb')

    def keys(self):
        return sorted(list(self._keys))

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
        return pickle.loads(self.db_txn.get(pickle.dumps(key)))

    def __setitem__(self, key, value):
        assert self.mode == 'w', 'can only write item in write mode'
        # in fact even key is __len__ it should be fine, because it's dumped in pickle mode.
        assert key not in ['__keys__'], f'{key} is internal variable, immutable to users'
        self.db_txn.put(pickle.dumps(key), pickle.dumps(value))
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