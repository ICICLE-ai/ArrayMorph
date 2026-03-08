# Required Python packages (install in .venv before running just):
#
# pip install \
#   build \
#   scikit-build-core \
#   cibuildwheel \
#   conan \
#   h5py==3.15.1

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

BUILD_TYPE := env_var_or_default("BUILD_TYPE", "Release")

# Automatically discover the HDF5 directory shipped with h5py.
# This will FAIL if:
#   • .venv does not exist
#   • h5py is not installed
# which is intentional.

H5PY_HDF5_DIR := `./.venv/bin/python3 -c "import h5py,os;d=os.path.dirname(h5py.__file__);print(os.path.join(d,'.dylibs') if os.path.exists(os.path.join(d,'.dylibs')) else os.path.join(os.path.dirname(d),'h5py.libs'))"`
CONAN_BUILD := justfile_directory() / "lib" / "build" / BUILD_TYPE
TOOLCHAIN := CONAN_BUILD / "generators" / "conan_toolchain.cmake"

default:
    @just --list

info:
    @echo "BUILD_TYPE: {{ BUILD_TYPE }}"
    @echo "H5PY_HDF5_DIR: {{ H5PY_HDF5_DIR }}"
    @echo "CONAN_BUILD: {{ CONAN_BUILD }}"
    @echo "TOOLCHAIN: {{ TOOLCHAIN }}"

deps:
    conan install lib --build=missing -s build_type={{ BUILD_TYPE }}

configure: deps
    H5PY_HDF5_DIR={{ H5PY_HDF5_DIR }} \
    cmake -S lib -B {{ CONAN_BUILD }} \
      -DCMAKE_BUILD_TYPE={{ BUILD_TYPE }} \
      -DCMAKE_TOOLCHAIN_FILE={{ TOOLCHAIN }}

build: configure
    cmake --build {{ CONAN_BUILD }}

wheel: deps
    CMAKE_TOOLCHAIN_FILE={{ TOOLCHAIN }} \
    H5PY_HDF5_DIR={{ H5PY_HDF5_DIR }} \
    ./.venv/bin/python -m build --wheel --no-isolation

clean:
    rm -rf \
        lib/build \
        dist \
        wheelhouse \
        *.egg-info
