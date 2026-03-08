import os

import h5py


base = os.path.dirname(h5py.__file__)
dylibs = os.path.join(base, ".dylibs")
libs = os.path.join(os.path.dirname(base), "h5py.libs")
print(dylibs if os.path.exists(dylibs) else libs)
