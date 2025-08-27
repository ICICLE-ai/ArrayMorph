# =============================================================================
# BUILD STAGE
# =============================================================================
FROM ubuntu:24.04 AS builder

# Install system dependencies
RUN apt update && apt install -y \
    build-essential \
    cmake \
    git \
    curl \
    pkg-config \
    ninja-build \
    python3 \
    python3-dev \
    python3-venv \
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

# Build HDF5 from source
RUN curl -L -o hdf5-1_14_2.tar.gz https://github.com/HDFGroup/hdf5/releases/download/hdf5-1_14_2/hdf5-1_14_2.tar.gz
RUN tar -xzf hdf5-1_14_2.tar.gz
RUN cd hdfsrc
WORKDIR /hdf5/hdfsrc
RUN ./configure
WORKDIR /hdf5/hdfsrc/build
RUN cmake -DCMAKE_INSTALL_PREFIX=/hdf5-1.14.2 ..
RUN make
RUN make install

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /
# Clone ArrayMorph repository
RUN git clone https://github.com/ICICLE-ai/ArrayMorph.git -b conan

# Build ArrayMorph
WORKDIR /ArrayMorph/arraymorph
ENV HDF5_DIR=/hdf5-1.14.2

COPY ./uv.lock .
COPY ./pyproject.toml .

RUN uv sync --frozen --no-cache
RUN conan profile detect --force




## Build AWS SDK for C++
#RUN git clone https://github.com/Microsoft/vcpkg.git
#WORKDIR /vcpkg
#RUN ./bootstrap-vcpkg.sh
#RUN ./vcpkg integrate install
#RUN ./vcpkg install aws-sdk-cpp
#RUN ./vcpkg install azure-identity-cpp azure-storage-blobs-cpp

#WORKDIR /
## Clone ArrayMorph repository
#RUN git clone https://github.com/ICICLE-ai/ArrayMorph.git
#
## Build ArrayMorph
#WORKDIR /ArrayMorph/arraymorph/build
#ENV HDF5_DIR=/hdf5-1.14.2
#
#RUN cmake .. -DCMAKE_INSTALL_PREFIX=/arraymorph \
#    -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake
#
#RUN make
#
## Collect only the runtime dependencies that ArrayMorph actually needs
#RUN mkdir -p /runtime-libs
#RUN find /ArrayMorph/arraymorph/build/src -type f -executable -exec ldd {} \; | \
#    grep "/vcpkg/" | \
#    awk '{print $3}' | \
#    sort -u | \
#    xargs -I {} cp {} /runtime-libs/ || true
#
## Explicitly copy AWS SDK libraries (they might be loaded dynamically)
#RUN find /vcpkg/installed -name "libaws-*.so*" -exec cp {} /runtime-libs/ \; || true
#RUN find /vcpkg/installed -name "libAWS*.so*" -exec cp {} /runtime-libs/ \; || true
#
## Explicitly copy Azure libraries
#RUN find /vcpkg/installed -name "libazure-*.so*" -exec cp {} /runtime-libs/ \; || true
#RUN find /vcpkg/installed -name "libAzure*.so*" -exec cp {} /runtime-libs/ \; || true
#
## Copy common dependencies that might not show up in ldd
#RUN find /vcpkg/installed -name "libcurl*.so*" -exec cp {} /runtime-libs/ \; || true
#RUN find /vcpkg/installed -name "libssl*.so*" -exec cp {} /runtime-libs/ \; || true
#RUN find /vcpkg/installed -name "libcrypto*.so*" -exec cp {} /runtime-libs/ \; || true
#
## Debug: Show what we're copying
#RUN echo "Runtime libraries collected:" && ls -la /runtime-libs/
## =============================================================================
## RUNTIME STAGE (More selective)
## =============================================================================
#FROM ubuntu:24.04 AS runtime
#
## Install minimal runtime dependencies
#RUN apt-get update && apt-get install -y \
#    libcurl4 \
#    libssl3 \
#    libuuid1 \
#    zlib1g \
#    && rm -rf /var/lib/apt/lists/*
#
## Copy only the runtime libraries that ArrayMorph actually needs
#COPY --from=builder /runtime-libs/ /usr/local/lib/
#
## Copy ArrayMorph build
#COPY --from=builder /ArrayMorph/arraymorph/build/src/ /usr/local/lib/arraymorph/
#
## Copy HDF5
#COPY --from=builder /hdf5-1.14.2/ /usr/local/lib/hdf5-1.14.2/
#
## Set up environment variables for runtime
#ENV HDF5_DIR=/usr/local/lib/hdf5-1.14.2
#ENV HDF5_PLUGIN_PATH=/usr/local/lib/hdf5-1.14.2/lib/plugin
#ENV HDF5_VOL_CONNECTOR=/usr/local/lib/arraymorph
#ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib/hdf5-1.14.2/lib:/usr/local/lib/arraymorph:$LD_LIBRARY_PATH
#
## Update library cache
#RUN ldconfig
#
## Default command
#CMD ["/bin/bash"]
