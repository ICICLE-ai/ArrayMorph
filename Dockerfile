# =============================================================================
# BUILD STAGE
# =============================================================================
FROM ubuntu:24.04 AS builder
ARG TARGETARCH

# Install system dependencies
RUN apt update && apt install -y \
    build-essential \
    cmake \
    git \
    curl \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    zlib1g-dev \
    liblzma-dev \
    uuid-dev \
    zip \
    unzip \
    tar \
    linux-libc-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /hdf5

# Build HDF5 1.14.2 from source with proper configuration
RUN curl -L -o hdf5-1_14_2.tar.gz https://github.com/HDFGroup/hdf5/releases/download/hdf5-1_14_2/hdf5-1_14_2.tar.gz
RUN tar -xzf hdf5-1_14_2.tar.gz

WORKDIR /hdf5/hdfsrc
# Configure with proper options for h5py compatibility
RUN ./configure \
    --prefix=/hdf5-1.14.2 \
    --enable-shared \
    --enable-build-mode=production \
    --enable-tools \
    --enable-static=no \
    --with-zlib \
    --with-szlib

# Build and install HDF5
RUN make -j$(nproc)
RUN make install

# Create the plugin directory that h5py expects
RUN mkdir -p /hdf5-1.14.2/lib/plugin

# Verify the HDF5 build
RUN ls -la /hdf5-1.14.2/lib/
RUN /hdf5-1.14.2/bin/h5dump --version

WORKDIR /

# Build AWS SDK for C++
RUN git clone https://github.com/Microsoft/vcpkg.git
WORKDIR /vcpkg
RUN ./bootstrap-vcpkg.sh
RUN ./vcpkg integrate install
# Set vcpkg triplet based on target architecture
RUN if [ "$TARGETARCH" = "arm64" ]; then \
    VCPKG_TRIPLET="arm64-linux-dynamic"; \
    elif [ "$TARGETARCH" = "386" ]; then \
    VCPKG_TRIPLET="x86-linux-dynamic"; \
    else \
    VCPKG_TRIPLET="x64-linux-dynamic"; \
    fi && \
    ./vcpkg install aws-sdk-cpp --triplet=$VCPKG_TRIPLET && \
    ./vcpkg install azure-identity-cpp azure-storage-blobs-cpp --triplet=$VCPKG_TRIPLET

WORKDIR /
# Clone ArrayMorph repository
RUN git clone https://github.com/ICICLE-ai/ArrayMorph.git

# Build ArrayMorph
WORKDIR /ArrayMorph/arraymorph/build

# Set comprehensive HDF5 environment variables for CMake
ENV HDF5_DIR=/hdf5-1.14.2
ENV HDF5_ROOT=/hdf5-1.14.2
ENV CMAKE_PREFIX_PATH=/hdf5-1.14.2:$CMAKE_PREFIX_PATH
ENV PKG_CONFIG_PATH=/hdf5-1.14.2/lib/pkgconfig:$PKG_CONFIG_PATH

RUN if [ "$TARGETARCH" = "arm64" ]; then \
    VCPKG_TRIPLET="arm64-linux-dynamic"; \
    elif [ "$TARGETARCH" = "386" ]; then \
    VCPKG_TRIPLET="x86-linux-dynamic"; \
    else \
    VCPKG_TRIPLET="x64-linux-dynamic"; \
    fi && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/arraymorph \
    -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_TARGET_TRIPLET=$VCPKG_TRIPLET \
    -DHDF5_ROOT=/hdf5-1.14.2 \
    -DHDF5_DIR=/hdf5-1.14.2 \
    -DHDF5_INCLUDE_DIRS=/hdf5-1.14.2/include \
    -DHDF5_LIBRARIES=/hdf5-1.14.2/lib/libhdf5.so \
    -DHDF5_C_LIBRARIES=/hdf5-1.14.2/lib/libhdf5.so

RUN make

# Debug: Check where vcpkg actually installed libraries
RUN echo "=== VCPKG STRUCTURE ===" && find /vcpkg -name "*.so*" | head -20
RUN echo "=== VCPKG INSTALLED STRUCTURE ===" && ls -la /vcpkg/installed/ || true
RUN echo "=== CHECKING FOR TRIPLET DIRS ===" && ls -la /vcpkg/installed/*/lib/ || true

# Collect only the runtime dependencies that ArrayMorph actually needs
RUN mkdir -p /runtime-libs

# First, find the correct vcpkg triplet directory
RUN TRIPLET_DIR=$(find /vcpkg/installed -name "lib" -type d | head -1 | xargs dirname) && \
    echo "Using triplet directory: $TRIPLET_DIR" && \
    find $TRIPLET_DIR/lib -name "*.so*" -exec cp {} /runtime-libs/ \; || true

# Also check for dependencies from ldd
RUN find /ArrayMorph/arraymorph/build/src -type f -executable -exec ldd {} \; | \
    grep "/vcpkg/" | \
    awk '{print $3}' | \
    sort -u | \
    xargs -I {} cp {} /runtime-libs/ || true

# Debug: Show what we're copying
RUN echo "Runtime libraries collected:" && ls -la /runtime-libs/

# =============================================================================
# RUNTIME STAGE
# =============================================================================
FROM ubuntu:24.04 AS runtime

# Install runtime AND build dependencies (needed for h5py compilation)
RUN apt-get update && apt-get install -y \
    libcurl4 \
    libssl3 \
    libuuid1 \
    zlib1g \
    python3-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy uv binaries
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy only the runtime libraries that ArrayMorph actually needs
COPY --from=builder /runtime-libs/ /usr/local/lib/

# Copy ArrayMorph build
COPY --from=builder /ArrayMorph/arraymorph/build/src/ /usr/local/lib/arraymorph/

# Copy HDF5 1.14.2
COPY --from=builder /hdf5-1.14.2/ /usr/local/lib/hdf5-1.14.2/

# Ensure plugin directory exists
RUN mkdir -p /usr/local/lib/hdf5-1.14.2/lib/plugin

# Set comprehensive environment variables for HDF5 1.14.2
ENV HDF5_DIR=/usr/local/lib/hdf5-1.14.2
ENV HDF5_ROOT=/usr/local/lib/hdf5-1.14.2
ENV HDF5_VERSION=1.14.2
ENV HDF5_MPI=OFF
ENV HDF5_PLUGIN_PATH=/usr/local/lib/arraymorph
ENV HDF5_VOL_CONNECTOR=arraymorph
ENV HDF5_USE_FILE_LOCKING=FALSE
ENV HDF5_DISABLE_VERSION_CHECK=1
ENV H5PY_SETUP_REQUIRES=0
ENV PKG_CONFIG_PATH=/usr/local/lib/hdf5-1.14.2/lib/pkgconfig:$PKG_CONFIG_PATH
ENV CPATH=/usr/local/lib/hdf5-1.14.2/include:$CPATH
ENV LIBRARY_PATH=/usr/local/lib/hdf5-1.14.2/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib/hdf5-1.14.2/lib:/usr/local/lib/arraymorph:$LD_LIBRARY_PATH

# Update library cache
RUN ldconfig

# Verify HDF5 is working and accessible
RUN /usr/local/lib/hdf5-1.14.2/bin/h5dump --version
RUN echo "HDF5 libraries:" && ls -la /usr/local/lib/hdf5-1.14.2/lib/libhdf5*

# Create symlink for HDF5 VOL plugin naming convention
RUN cd /usr/local/lib/arraymorph && \
    ln -s libarraymorph.so.1 libhdf5_vol_arraymorph.so
    
# Create Python virtual environment and install dependencies
WORKDIR /app
RUN uv venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install basic Python packages first
RUN uv pip install numpy cython setuptools wheel

# Now install h5py from source with your HDF5 1.14.2
# Force environment variables for h5py build
ENV H5PY_SETUP_REQUIRES=0
RUN HDF5_VERSION=1.14.2 uv pip install --no-binary=h5py h5py

# Verify h5py installation works with your HDF5
RUN python3 -c "import h5py; print(f'h5py version: {h5py.version.version}'); print(f'HDF5 version: {h5py.version.hdf5_version}')"

# Default command
CMD ["/bin/bash"]
