cmake_minimum_required(VERSION 3.20)

project(ArrayMorph VERSION 0.2.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif()

include_directories(${CMAKE_SOURCE_DIR}/include)

# Dependencies from Conan/CMakeDeps
find_package(AWSSDK REQUIRED COMPONENTS core s3)
find_package(AzureSDK REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

# We keep Conan HDF5 only for headers.
find_package(HDF5 REQUIRED COMPONENTS C)

# Need Python so we can discover h5py's bundled HDF5 if H5PY_HDF5_DIR
# is not passed in explicitly.
find_package(Python REQUIRED COMPONENTS Interpreter)

# Conan's AWS package injects `c++` as a system lib on macOS, but AppleClang
# already links libc++ when using `-stdlib=libc++`, which causes a duplicate
# library warning at link time.
function(arraymorph_remove_interface_link_item target item)
    if(NOT TARGET "${target}")
        return()
    endif()

    get_target_property(_link_libs "${target}" INTERFACE_LINK_LIBRARIES)
    if(NOT _link_libs)
        return()
    endif()

    list(REMOVE_ITEM _link_libs "${item}")
    set_property(
        TARGET "${target}"
        PROPERTY INTERFACE_LINK_LIBRARIES "${_link_libs}"
    )
endfunction()

if(APPLE)
    arraymorph_remove_interface_link_item(AWS::aws-sdk-cpp-core "c++")
    arraymorph_remove_interface_link_item(AWS::aws-sdk-cpp-s3 "c++")
    arraymorph_remove_interface_link_item(aws-sdk-cpp::aws-sdk-cpp "c++")
endif()

# ------------------------------------------------------------
# HDF5 strategy:
#   - headers come from Conan's HDF5 package
#   - runtime libhdf5 comes from the installed h5py wheel
# ------------------------------------------------------------

set(H5PY_LIB_DIR "$ENV{H5PY_HDF5_DIR}")

if(NOT H5PY_LIB_DIR)
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -c
        "import h5py, os; d=os.path.dirname(h5py.__file__); dylibs=os.path.join(d,'.dylibs'); libs=os.path.join(os.path.dirname(d),'h5py.libs'); print(dylibs if os.path.exists(dylibs) else libs)"
        OUTPUT_VARIABLE H5PY_LIB_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        COMMAND_ERROR_IS_FATAL ANY
    )
endif()

if(NOT EXISTS "${H5PY_LIB_DIR}")
    message(FATAL_ERROR
        "Could not locate h5py's bundled HDF5 directory.\n"
        "Set H5PY_HDF5_DIR or install h5py in the build environment."
    )
endif()

file(GLOB _h5py_hdf5_libs
    "${H5PY_LIB_DIR}/libhdf5.dylib"
    "${H5PY_LIB_DIR}/libhdf5.*.dylib"
    "${H5PY_LIB_DIR}/libhdf5.so"
    "${H5PY_LIB_DIR}/libhdf5.so.*"
    "${H5PY_LIB_DIR}/libhdf5-*.so"
    "${H5PY_LIB_DIR}/libhdf5-*.so.*"
)

if(NOT _h5py_hdf5_libs)
    file(GLOB _h5py_dir_contents "${H5PY_LIB_DIR}/*")
    message(FATAL_ERROR
        "No core HDF5 shared library found in H5PY_HDF5_DIR=${H5PY_LIB_DIR}\n"
        "Directory contents: ${_h5py_dir_contents}"
    )
endif()

list(GET _h5py_hdf5_libs 0 _h5py_hdf5_lib)

add_library(hdf5_runtime SHARED IMPORTED GLOBAL)
set_target_properties(hdf5_runtime PROPERTIES
    IMPORTED_LOCATION "${_h5py_hdf5_lib}"
    INTERFACE_INCLUDE_DIRECTORIES "${HDF5_INCLUDE_DIRS}"
)

message(STATUS "HDF5 headers (Conan): ${HDF5_INCLUDE_DIRS}")
message(STATUS "HDF5 runtime (h5py):  ${_h5py_hdf5_lib}")

set(ALL_DEPS
    AWS::aws-sdk-cpp-core
    AWS::aws-sdk-cpp-s3
    Azure::azure-storage-blobs
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl
    hdf5_runtime
)

add_subdirectory(src)
