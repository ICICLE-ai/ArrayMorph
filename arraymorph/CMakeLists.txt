cmake_minimum_required(VERSION 3.20)

project(ArrayMorph
    VERSION 0.1.0
    LANGUAGES C CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)  # b_staticpic=true
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build type and optimization
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif()

# Include directory
include_directories(${CMAKE_SOURCE_DIR}/include)

# --- Find External Dependencies ---

# AWS SDK
find_package(AWSSDK REQUIRED COMPONENTS core s3)

# Azure SDK
find_package(AzureSDK CONFIG REQUIRED)

# cURL and OpenSSL
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

if(NOT DEFINED ENV{H5PY_HDF5_DIR})
  message(FATAL_ERROR "H5PY_HDF5_DIR environment variable not set")
endif()

# --- HDF5 with h5py workaround ---
set(H5PY_LIB_DIR "$ENV{H5PY_HDF5_DIR}")

# Find HDF5 for headers (via Conan or system)
find_package(HDF5 REQUIRED COMPONENTS C)

# Create custom HDF5 dependency: Conan headers + h5py binary
add_library(hdf5_custom INTERFACE)
target_include_directories(hdf5_custom INTERFACE ${HDF5_INCLUDE_DIRS})
target_link_directories(hdf5_custom INTERFACE ${H5PY_LIB_DIR})
target_link_libraries(hdf5_custom INTERFACE hdf5.310)

# Print confirmation
message(STATUS "HDF5 found: ${HDF5_VERSION}")

# Collect all dependencies into a list
set(ALL_DEPS
    AWS::aws-sdk-cpp-core
    AWS::aws-sdk-cpp-s3
    Azure::azure-storage-blobs
    hdf5_custom
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl
)

# Enter src directory 
add_subdirectory(src)
