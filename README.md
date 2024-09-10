# About ArrayMorph

ArrayMorph stores and retrieves HDF5 data from the AWS S3 cloud object store. It supports both the HDF5 C++ API and the h5py Python API. ArrayMorph is an HDF5 VOL plugin that is loaded dynamically (for HDF5 v1.14 or newer) without requiring further changes to applications.

## Requirements

	CMake>=3.0
	h5py>=3.11.0 (if using python)
	HDF5=1.14.3
	aws-sdk-cpp-s3=1.9.379

## Install dependencies

It is recommended to use Conda to install and manage dependencies for ArrayMorph.

	1. Download and install Conda following instructions from Conda official website (https://docs.anaconda.com/miniconda/)
	2. Run the following commands to install all dependencies for ArrayMorph
		$ conda create -n arraymorph conda-forge::gxx=8
		$ conda install -n arraymorph cmake conda-forge::hdf5=1.14 conda-forge::aws-sdk-cpp=1.9.379 conda-forge::h5py

### Build ArrayMorph

	1. Activate the conda environment with all the dependencies
		$ conda activate arraymorph
	2. Download and build ArrayMorph
		$ git clone https://github.com/ICICLE-ai/arraymorph.git
		$ cd arraymorph/arraymorph
		$ cmake -B ./build -S . -DCMAKE_PREFIX_PATH=$CONDA_PREFIX
		$ cd build
		$ make

## Configuration
    
To run ArrayMorph, you need to create access keys from the AWS console, and an empty S3 bucket.

	# Enable VOL plugin of ArrayMoprh
	export HDF5_PLUGIN_PATH=/path/to/arraymorph/arraymorph/build/src
	export HDF5_VOL_CONNECTOR=arraymorph

	# Configure S3
	export BUCKET_NAME=XXXXXX
	export AWS_ACCESS_KEY_ID=XXXXXX
	export AWS_SECRET_ACCESS_KEY=XXXXXX
	export AWS_REGION=us-east-2 # change to AWS region of your bucket

## Run a simple example about writing and reading hdf5 files from cloud

	1. Activate conda environment
		$ conda activate arraymorph
	2. Run script to write sample hdf5 data to cloud
		$ cd examples/python
		$ python3 write.py
	3. Run script to read data from cloud hdf5 file
		$ cd examples/python
		$ python3 read.py
