import json
import os
import re
import subprocess
import sys
from collections import OrderedDict


want_arraymorph = len(sys.argv) > 1 and sys.argv[1] == "1"

import h5py  # noqa: F401

if want_arraymorph:
    import arraymorph  # noqa: F401

paths = []
if sys.platform.startswith("linux"):
    with open("/proc/self/maps", "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            match = re.search(r"(/[^\s]*libhdf5[^\s]*)", line)
            if match:
                paths.append(match.group(1))
elif sys.platform == "darwin":
    out = subprocess.check_output(
        ["vmmap", str(os.getpid())],
        text=True,
        stderr=subprocess.DEVNULL,
    )
    for line in out.splitlines():
        match = re.search(r"(/.*libhdf5[^\s]*)", line)
        if match:
            paths.append(match.group(1))
else:
    raise SystemExit(f"Unsupported platform for this helper: {sys.platform}")

unique = list(OrderedDict.fromkeys(paths))
print(
    json.dumps(
        {
            "imported_arraymorph": want_arraymorph,
            "loaded_hdf5": unique,
            "unique_hdf5_count": len(unique),
        },
        indent=2,
    )
)
