import os
import os.path as osp
import sys
import glob
from PIL import Image
import six
import string

import tqdm
import lmdb
from lmdbdict import LMDBDict

import torch
import torch.utils.data as data
from torch.utils.data import DataLoader
from torchvision.transforms import transforms
from torchvision.datasets import ImageFolder
from torchvision import transforms, datasets

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

# With original lmdb write
def folder2lmdb(directory, lmdb_path, write_frequency=2500, num_workers=16):
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


# With lmdbdict
def folder2lmdb_(directory, lmdb_path, write_frequency=2500, num_workers=16):
    print("Loading dataset from %s" % directory)
    dataset = Folder(directory)
    print(f"Found {len(dataset)} files")
    data_loader = DataLoader(dataset, num_workers=num_workers, collate_fn=lambda x: x)

    print("Generate LMDB to %s" % lmdb_path)
    db = LMDBDict(lmdb_path, mode='w')
    
    print(len(dataset), len(data_loader))
    keys = []
    for idx, data in enumerate(tqdm.tqdm(data_loader)):
        # print(type(data), data)
        fn, rawbyte = data[0]
        db[fn] = rawbyte
        keys.append(fn)
        if (idx+1) % write_frequency == 0:
            print("[%d/%d]" % (idx, len(data_loader)))
            db.flush()

    # finish iterating through dataset
    print("Flushing database ...")
    del db

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--f", "--folder", type=str)
    parser.add_argument('--lmdb', type=str, required=True)
    parser.add_argument('-p', '--procs', type=int, default=20)

    args = parser.parse_args()

    folder2lmdb_(args.folder, args.out,  num_workers=args.procs)
    # x = LMDBDict(args.out)
    # import pudb;pu.db
