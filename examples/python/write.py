import h5py
import numpy as np
import sys
print("before open", file=sys.stderr, flush=True)
f = h5py.File("demo-mac.h5", "w")
dset = f.create_dataset("test", (100, 100), chunks=(10, 10), dtype="i4")

# write 100 x 100 2d arrays filled with value 1
print("before open")
with f:
    print("after open")
    dset = f.create_dataset("x", data=[1, 2, 3])
    print("after create_dataset")
print("after close")
