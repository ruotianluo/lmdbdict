import os
import os.path as osp
import sys
import glob
from PIL import Image
import six
import string

import lmdb
import pickle
import tqdm

import torch
import torch.utils.data as data
from torch.utils.data import DataLoader
from torchvision.transforms import transforms
from torchvision.datasets import ImageFolder
from torchvision import transforms, datasets


class LMDBDict:
    def __init__(self, lmdb_path, mode='r'):
        self.lmdb_path = lmdb_path
        self.mode = mode
        self._init_db()
        self._length = pickle.loads(self.db_txn.get(b'__len__'))
        self._keys = pickle.loads(self.db_txn.get(b'__keys__'))

    def keys(self):
        return self._keys

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

    def __getitem__(self, key):
        if key not in self:
            raise KeyError
        byteflow = self.db_txn.get(pickle.dumps(key))
        return byteflow 

    def __setitem__(self, key, value):
        return
        # in fact even key is __len__ it should be fine, because it's dumped in pickle mode.
        assert key not in ['__len__', '__keys__'], f'{key} is internal variable, immutable to users'
        self.db_txn.put(pickle.dumps(key), value)

    @classmethod
    def __delitem__(cls, key):
        self.db_txn.delete(pickle.dumps(key))

    def __len__(self):
        return self._length

    def __repr__(self):
        return self.__class__.__name__ + ' (' + self.lmdb_path + ')'

    def __del__(self):
        # make sure the the __key__ and __len__ are updated
        # and it's flushed
        return
        self.env.sync()
        self.env.close()


class Folder(data.Dataset):

    def __init__(self, root):
        super(Folder, self).__init__()
        self.root = root

        self.files = glob.glob(root+'/**/*', recursive=True) 
        self.files = [os.path.relpath(_, root) for _ in self.files]

    def __getitem__(self, index):
        """
        Args:
            index (int): Index
        Returns:
            tuple: (sample, target) where target is class_index of the target class.
        """
        path = os.path.join(self.root, self.files[index])
        return self.files[index], open(path, 'rb').read()

    def __len__(self):
        return len(self.files)


def folder2lmdb(directory, lmdb_path, write_frequency=5000, num_workers=16):
    print("Loading dataset from %s" % directory)
    dataset = Folder(directory)
    print(f"Found {len(dataset)} files")
    data_loader = DataLoader(dataset, num_workers=num_workers, collate_fn=lambda x: x)

    print("Generate LMDB to %s" % lmdb_path)
    db = lmdb.open(lmdb_path, subdir=False,
                   map_size=1099511627776 * 2, readonly=False,
                   meminit=False, map_async=True)
    
    print(len(dataset), len(data_loader))
    txn = db.begin(write=True)
    keys = []
    for idx, data in enumerate(tqdm.tqdm(data_loader)):
        # print(type(data), data)
        fn, rawbyte = data[0]
        txn.put(pickle.dumps(fn), rawbyte)
        keys.append(fn)
        if idx % write_frequency == 0:
            print("[%d/%d]" % (idx, len(data_loader)))
            txn.commit()
            txn = db.begin(write=True)

    # finish iterating through dataset
    txn.commit()
    with db.begin(write=True) as txn:
        txn.put(b'__keys__', pickle.dumps(keys))
        txn.put(b'__len__', pickle.dumps(len(keys)))

    print("Flushing database ...")
    db.sync()
    db.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--folder", type=str)
    parser.add_argument('--out', type=str, required=True)
    parser.add_argument('-p', '--procs', type=int, default=20)

    args = parser.parse_args()

    folder2lmdb(args.folder, args.out,  num_workers=args.procs)
    x = LMDBDict(args.out)
    import pudb;pu.db
