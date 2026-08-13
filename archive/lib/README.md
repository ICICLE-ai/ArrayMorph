# lib — ArrayMorph C++ shared library

This directory contains the C++ source code and CMake build system for `lib_arraymorph`, the HDF5 VOL connector plugin.

## Directory layout

```
lib/
├── src/              # C++ source files
├── include/          # Public headers
├── CMakeLists.txt    # CMake build definition
└── vcpkg.json        # vcpkg dependency manifest (AWS SDK, Azure SDK)
```

## Download a pre-built binary

Each [GitHub release](https://github.com/ICICLE-ai/ArrayMorph/releases) attaches standalone pre-compiled binaries — no build toolchain required:

| File | Platform |
|---|---|
| `lib_arraymorph-linux-x86_64.so` | Linux x86_64 |
| `lib_arraymorph-linux-aarch64.so` | Linux aarch64 |
| `lib_arraymorph-macos-arm64.dylib` | macOS Apple Silicon |

Download the file for your platform from the release assets and point `HDF5_PLUGIN_PATH` at the containing directory.

The standalone binary still requires an HDF5 shared library at runtime. Set `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS) to the directory containing `libhdf5.so` / `libhdf5.dylib` before loading the plugin.

## Prerequisites

- [vcpkg](https://github.com/microsoft/vcpkg) — installs the AWS and Azure C++ SDKs via CMake
- [CMake](https://cmake.org) and [Ninja](https://ninja-build.org)
- HDF5 shared library (`.so` / `.dylib`) — set `HDF5_DIR` to the directory containing it

## Build

```bash
cmake -B build -S . \
    -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT:-~/.vcpkg}/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -G Ninja

cmake --build build
```

This produces `build/lib_arraymorph.dylib` on macOS or `build/lib_arraymorph.so` on Linux.

### Locating HDF5

`lib_arraymorph` must link against an existing HDF5 shared library. Set `HDF5_DIR` to the directory containing the HDF5 `.so` / `.dylib` before running CMake:

```bash
export HDF5_DIR=/path/to/hdf5/lib
```

If you have h5py installed in a Python environment, you can point directly at its bundled libraries:

```bash
# macOS
export HDF5_DIR=/path/to/.venv/lib/python3.x/site-packages/h5py/.dylibs

# Linux
export HDF5_DIR=/path/to/.venv/lib/python3.x/site-packages/h5py.libs
```
