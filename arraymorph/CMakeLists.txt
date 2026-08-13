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
find_package(azure-core-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

# cURL and OpenSSL
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

# Find HDF5 for headers (via Conan or system)
find_package(HDF5 REQUIRED COMPONENTS C)

# Inject the found HDF5 headers into the global include path
include_directories(${HDF5_INCLUDE_DIRS})

# Print confirmation
message(STATUS "HDF5 found: ${HDF5_VERSION}")

# Create an interface dependency target
add_library(arraymorph_dependencies INTERFACE)

# Bind all found library paths and targets directly to it
target_link_libraries(arraymorph_dependencies INTERFACE
    ${AWSSDK_LINK_LIBRARIES}
    Azure::azure-core
    Azure::azure-storage-blobs
    ${HDF5_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl
)

# Enter src directory 
add_subdirectory(src)
