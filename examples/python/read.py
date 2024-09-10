import h5py
import numpy as np

f = h5py.File('demo.h5', 'r')
dset = f['test']
print(dset.dtype)

print(dset[5:15,5:15])
