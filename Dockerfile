# -----------------------------
# Stage 1: build
# -----------------------------
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /work

RUN apt-get update && apt-get install -y \
  build-essential \
  cmake \
  ninja-build \
  pkg-config \
  git \
  curl \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Install uv

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml README.md /work/
COPY lib/conanfile.py /work/lib/conanfile.py

# Create environment
RUN uv python install 3.10
RUN uv venv --python 3.10 

# Install build deps
RUN uv pip install --python .venv/bin/python \
  build \
  scikit-build-core \
  conan \
  'h5py>=3.10,<3.16' \
  ninja \
  cmake

# Conan
RUN --mount=type=cache,target=/root/.conan2 \
  .venv/bin/conan profile detect --force
RUN --mount=type=cache,target=/root/.conan2 \
  .venv/bin/conan install lib --build=missing -s build_type=Release -of /work/lib/build

COPY src /work/src
COPY lib/CMakeLists.txt /work/lib/CMakeLists.txt
COPY lib/include /work/lib/include
COPY lib/src /work/lib/src

# Build wheel
RUN --mount=type=cache,target=/root/.conan2 \
  .venv/bin/python -c 'import h5py, os; d=os.path.dirname(h5py.__file__); libdir=os.path.join(d, ".dylibs") if os.path.exists(os.path.join(d, ".dylibs")) else os.path.join(os.path.dirname(d), "h5py.libs"); print(f"h5py {h5py.__version__} -> {libdir}")' && \
  export H5PY_HDF5_DIR="$(.venv/bin/python -c 'import h5py, os; d=os.path.dirname(h5py.__file__); print(os.path.join(d, ".dylibs") if os.path.exists(os.path.join(d, ".dylibs")) else os.path.join(os.path.dirname(d), "h5py.libs"))')" && \
  export CMAKE_ARGS="-DCMAKE_TOOLCHAIN_FILE=/work/lib/build/build/Release/generators/conan_toolchain.cmake" && \
  .venv/bin/python -m build --wheel --no-isolation

# -----------------------------
# Stage 2: runtime
# -----------------------------
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y \
  curl \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY --from=builder /work/dist /app/dist

RUN uv venv --python 3.10
RUN uv pip install --python .venv/bin/python dist/*.whl

ENV STORAGE_PLATFORM=S3
ENV AWS_REGION=us-east-1

COPY ./examples/python/write.py ./test.py

CMD bash -lc '\
  set -e; \
  PLUGIN_PATH=$(./.venv/bin/python -c "import arraymorph; print(arraymorph.get_plugin_path())"); \
  PLUGIN_DIR=$(./.venv/bin/python -c "import arraymorph; print(arraymorph.get_plugin_dir())"); \
  H5PY_DIR=$(./.venv/bin/python -c "import h5py, os; d=os.path.dirname(h5py.__file__); print(os.path.join(d,\".dylibs\") if os.path.exists(os.path.join(d,\".dylibs\")) else os.path.join(os.path.dirname(d),\"h5py.libs\"))"); \
  export HDF5_PLUGIN_PATH="$PLUGIN_DIR"; \
  export HDF5_VOL_CONNECTOR="arraymorph under_vol=0;under_info={}"; \
  export LD_LIBRARY_PATH="$H5PY_DIR:${LD_LIBRARY_PATH:-}"; \
  echo "plugin path: $PLUGIN_PATH"; \
  echo "plugin dir:  $PLUGIN_DIR"; \
  echo "h5py dir:    $H5PY_DIR"; \
  echo "--- plugin dir ---"; \
  ls -la "$PLUGIN_DIR"; \
  echo "--- h5py dir ---"; \
  ls -la "$H5PY_DIR"; \
  echo "--- ldd plugin ---"; \
  ldd "$PLUGIN_PATH" || true; \
  echo "--- h5py check ---"; \
  ./.venv/bin/python -c "import h5py; print(\"h5py\", h5py.__version__); print(\"hdf5\", h5py.version.hdf5_version)"; \
  echo "--- run test ---"; \
  ./.venv/bin/python -u test.py'

