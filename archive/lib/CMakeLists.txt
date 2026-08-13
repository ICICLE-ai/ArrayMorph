cmake_minimum_required(VERSION 3.20)

project(ArrayMorph
    VERSION 0.2.0
    LANGUAGES C CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
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
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

# cURL and OpenSSL
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

# --- HDF5: vcpkg headers + h5py runtime binary ---
#
# ArrayMorph is a VOL plugin that gets dlopen'd by HDF5 at runtime.
# We MUST link against the same HDF5 that h5py ships to avoid
# duplicate symbol conflicts. vcpkg provides headers, h5py provides
# the shared library.

# Accept HDF5_DIR from either CMake variable (-D) or env var
if(NOT HDF5_DIR AND DEFINED ENV{HDF5_DIR})
    set(HDF5_DIR "$ENV{HDF5_DIR}")
endif()

if(NOT HDF5_DIR)
    message(FATAL_ERROR
        "HDF5_DIR not set. Run:\n"
        "  export HDF5_DIR=$(python3 -c \""
        "import h5py, os; "
        "d=os.path.dirname(h5py.__file__); "
        "print(os.path.join(d,'.dylibs') if os.path.exists(os.path.join(d,'.dylibs')) "
        "else os.path.join(os.path.dirname(d),'h5py.libs'))\")"
    )
endif()

set(H5PY_LIB_DIR "${HDF5_DIR}")

# Find HDF5 headers (via vcpkg)
find_package(HDF5 REQUIRED COMPONENTS C)

# Locate the actual shared library in h5py's bundled directory
file(GLOB _h5py_hdf5_libs
    "${H5PY_LIB_DIR}/libhdf5*.dylib"   # macOS
    "${H5PY_LIB_DIR}/libhdf5*.so*"     # Linux
)

if(NOT _h5py_hdf5_libs)
    # List what's actually in the directory for debugging
    file(GLOB _h5py_dir_contents "${H5PY_LIB_DIR}/*")
    message(FATAL_ERROR
        "No HDF5 shared library found in HDF5_DIR=${H5PY_LIB_DIR}\n"
        "Directory contents: ${_h5py_dir_contents}\n"
        "Expected libhdf5*.dylib (macOS) or libhdf5*.so* (Linux)"
    )
endif()

# Pick the first match
list(GET _h5py_hdf5_libs 0 _h5py_hdf5_lib)

# Create imported target: vcpkg headers + h5py binary
add_library(hdf5_custom SHARED IMPORTED)
set_target_properties(hdf5_custom PROPERTIES
    IMPORTED_LOCATION "${_h5py_hdf5_lib}"
    INTERFACE_INCLUDE_DIRECTORIES "${HDF5_INCLUDE_DIRS}"
)

message(STATUS "HDF5 version: ${HDF5_VERSION}")
message(STATUS "HDF5 headers: ${HDF5_INCLUDE_DIRS}")
message(STATUS "HDF5 binary:  ${_h5py_hdf5_lib}")

# Collect all dependencies into a list
set(ALL_DEPS
    aws-cpp-sdk-core
    aws-cpp-sdk-s3
    Azure::azure-storage-blobs
    hdf5_custom
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl
)

# Enter src directory
add_subdirectory(src)
