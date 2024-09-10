import h5py
import numpy as np

f = h5py.File('demo.h5', 'w')
dset = f.create_dataset("test", (100, 100), chunks=(10, 10), dtype='i4')

# write 100 x 100 2d arrays filled with value 1
data = np.ones((100, 100), dtype='i4')
for i in range(100):
    for j in range(100):
        data[i][j] = i + j
dset[:] = data
