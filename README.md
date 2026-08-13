# ArrayMorph

[![Build Status](https://github.com/ICICLE-ai/arraymorph/actions/workflows/build.yaml/badge.svg)](https://github.com/ICICLE-ai/arraymorph/actions/workflows/build.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ArrayMorph is a software to manage array data stored on cloud object storage efficiently. It supports both HDF5 C++ API and h5py API. The data returned by h5py API is numpy arrays. By using h5py API, users can access array data stored on the cloud and feed the read data into machine learning pipelines seamlessly.

**Tag**: CI4AI

## References

- [HDF5 VOL connectors](https://docs.hdfgroup.org/hdf5/develop/_v_o_l.html)
- [AWS SDK for C++](https://github.com/aws/aws-sdk-cpp)
- [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp)
- [h5py documentation](https://docs.h5py.org/en/stable/)
- [conda-forge](https://conda-forge.org/)

## Acknowledgements

This project is supported by:

*National Science Foundation (NSF) funded AI institute for Intelligent Cyberinfrastructure with Computational Learning in the Environment (ICICLE) (OAC 2112606)*

## Issue reporting

  Please report issues via [GitHub Issues](https://github.com/ICICLE-ai/ArrayMorph/issues). Include your cloud
  backend, HDF5/h5py versions, steps to reproduce, and any error logs — with cloud credentials redacted.


---

# How-To Guides

## Install dependencies

It is recommended to use Conda (and conda-forge) for managing dependencies.

1. Install [Miniconda](https://docs.anaconda.com/miniconda/)  
2. Install [conda-build](https://docs.conda.io/projects/conda-build/en/stable/install-conda-build.html) for installing local conda packages
3. Create and activate environment with dependencies:
   ```bash
   conda create -n arraymorph
   conda activate arraymorph
   conda install -n arraymorph cmake conda-forge::hdf5=1.14.2 conda-forge::aws-sdk-cpp conda-forge::azure-core-cpp conda-forge::azure-storage-blobs-cpp conda-forge::h5py
   ```

## Install ArrayMorph via Conda
### Option 1: Build and Install from Source
Clone the repository and build the conda package locally:
   ```bash
    git clone https://github.com/ICICLE-ai/arraymorph.git
    cd arraymorph/arraymorph
    conda build -c conda-forge .
    conda install -n arraymorph arraymorph --use-local -c conda-forge
   ```

### Option 2: Install from Pre-built Local Package
We provide a pre-built conda package in the arraymorph_channel directory. Install it directly using:
   ```bash
   git clone https://github.com/ICICLE-ai/arraymorph.git
   cd arraymorph/arraymorph_channel
   conda index .
   conda install -n arraymorph arraymorph -c file://$(pwd) -c conda-forge
   ```

## Install ArryMorph from source code

### Build ArrayMorph
```bash
git clone https://github.com/ICICLE-ai/arraymorph.git
cd arraymorph/arraymorph
cmake -B ./build -S . -DCMAKE_PREFIX_PATH=$CONDA_PREFIX
cd build
make
```

### Enable VOL plugin:
```bash
export HDF5_PLUGIN_PATH=/path/to/arraymorph/arraymorph/build/src
export HDF5_VOL_CONNECTOR=arraymorph
```

## Configure Environment for Cloud Access
Create a JSON configuration file named `config` at `~/.arraymorph` with the following settings based on your cloud provider:
### AWS Configuration:
```json
{
    "STORAGE_PLATFORM": "S3",
    "BUCKET_NAME": "XXXXX",
    "AWS_ACCESS_KEY_ID": "XXXXX",
    "AWS_SECRET_ACCESS_KEY": "XXXXX",
    "AWS_REGION": "us-east-2"
}

```

### Azure Configuration:
```json
{
    "STORAGE_PLATFORM": "Azure",
    "BUCKET_NAME": "XXXXX",
    "AZURE_STORAGE_CONNECTION_STRING": "XXXXX"
}
```

### S3-compatible object store configuration (MinIO, Ceph, Garage):
Set `AWS_ENDPOINT_URL_S3`, `AWS_USE_PATH_STYLE`, and `AWS_SIGNED_PAYLOADS` to match the requirements of most self-hosted S3-compatible stores:
```json
{
    "STORAGE_PLATFORM": "S3",
    "BUCKET_NAME": "XXXXX",
    "AWS_ENDPOINT_URL_S3": "XXXXX",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_ACCESS_KEY_ID": "XXXXX",
    "AWS_SECRET_ACCESS_KEY": "XXXXX",
    "AWS_REGION": "XXXXX",
    "AWS_USE_PATH_STYLE": "true",
    "AWS_USE_TLS": "true",
    "AWS_SIGNED_PAYLOADS": "true"
}
```

## Enable ArrayMorph with h5py in Python
To enable ArrayMorph when using h5py, import the `arraymorph` package BEFORE importing `h5py`:

```python
import arraymorph
import h5py
```

---

# Tutorials

## Run a simple example: Writing and Reading HDF5 files from Cloud

### Prerequisites:
- AWS or Azure cloud account with credentials
- S3 bucket or Azure container
- ArrayMorph dependencies installed

### Steps:
1. Activate conda environment  
   ```bash
   conda activate arraymorph
   ```

2. Write sample HDF5 data to the cloud  
   ```bash
   cd examples/python
   python3 write.py
   ```

3. Read data back from cloud HDF5 file  
   ```bash
   cd examples/python
   python3 read.py
   ```
---

# Explanation

### How ArrayMorph Works

ArrayMorph plugs into the HDF5 stack using a VOL (Virtual Object Layer) plugin that intercepts file operations and routes them to cloud object storage instead of local files. This allows existing HDF5 APIs (both C++ and h5py in Python) to operate on cloud-based data seamlessly, enabling transparent cloud access for scientific or ML pipelines.

It supports:
- Cloud backends: AWS S3, Azure Blob, and S3-compatible storage
- File formats: Current binary data stream (we plan to extend to other formats like jpg in the future)
- Languages: C++ and Python (via h5py compatibility)

The system is designed to be efficient in latency-sensitive scenarios and aims to integrate well with large-scale distributed training and inference.

