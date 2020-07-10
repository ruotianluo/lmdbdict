# lmdbdict

This is a lib trying to make lmdb behaved like a python dict.

Many adopted from https://github.com/Lyken17/Efficient-PyTorch.

# Install
pip install git+

# How to use

```
from lmdbdict import LMDBDict

lmdbpath = 'tmplmdb.lmdb'
# In write mode, you can modify keys
# keys and values can be any pickable objects
d = LMDBDict(lmdbpath, mode = 'w')
d[1] = 2; d[2] = 3
list(d.keys())
d.values() # not supported
del d[2]
# you can flush the d to make sure the data is written to the disk
d.flush()
# delete the d will also flush
del d

# In read mode, you can only read
d = LMDBDict(lmdbpath, mode = 'r')
d[1]
```