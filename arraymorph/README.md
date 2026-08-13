# Building ArrayMorph into a conda package

This folder contains the ArrayMorph source code (./src/), the CMake file to build ArrayMorph (CMakeLists.txt) and the Conda build recipes (build.sh, meta.yaml).

## Build ArrayMorph conda package

1. Install [Miniconda](https://docs.anaconda.com/miniconda/)
2. Install [conda-build](https://docs.conda.io/projects/conda-build/en/stable/install-conda-build.html)
3. Update conda and conda-build
4. Under the current folder, build ArrayMorph conda pacakge
   ```bash
   conda build -c conda-forge .
   ```

## Get ArrayMorph conda package

ArrayMorph conda package is stored in /path/to/conda/conda-bld/linux-64/
