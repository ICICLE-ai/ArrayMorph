# Internal include directories
set(PROJECT_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

# Propagate Conan-provided include dirs, definitions, and link requirements
# to any target that includes third-party headers.
add_library(arraymorph_deps INTERFACE)
target_link_libraries(arraymorph_deps INTERFACE ${ALL_DEPS})

# Internal static libraries
add_library(utils STATIC core/utils.cc)
target_include_directories(utils PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(utils PRIVATE arraymorph_deps)

add_library(constants STATIC core/constants.cc)
target_include_directories(constants PUBLIC ${PROJECT_INCLUDE_DIRS})

add_library(operators STATIC core/operators.cc)
target_include_directories(operators PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(operators PRIVATE constants arraymorph_deps)

add_library(chunk_obj STATIC s3vl/chunk_obj.cc)
target_include_directories(chunk_obj PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(chunk_obj PRIVATE operators utils arraymorph_deps)

add_library(dataset_obj STATIC s3vl/dataset_obj.cc)
target_include_directories(dataset_obj PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(dataset_obj PRIVATE chunk_obj arraymorph_deps)

add_library(file_callbacks STATIC s3vl/file_callbacks.cc)
target_include_directories(file_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(file_callbacks PRIVATE operators arraymorph_deps)

add_library(dataset_callbacks STATIC s3vl/dataset_callbacks.cc)
target_include_directories(dataset_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(dataset_callbacks PRIVATE file_callbacks dataset_obj arraymorph_deps)

add_library(group_callbacks STATIC s3vl/group_callbacks.cc)
target_include_directories(group_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(group_callbacks PRIVATE dataset_callbacks arraymorph_deps)

# Final VOL connector shared library
add_library(arraymorph SHARED s3vl/vol_connector.cc)
set_target_properties(arraymorph PROPERTIES PREFIX "lib")
target_include_directories(arraymorph PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(arraymorph PUBLIC
    group_callbacks
    arraymorph_deps
)

# Runtime search path so the plugin can find h5py's bundled HDF5
# after installation into site-packages/arraymorph/lib.
if(APPLE)
    set_target_properties(arraymorph PROPERTIES
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_RPATH "@loader_path/../../h5py/.dylibs"
    )
elseif(UNIX)
    set_target_properties(arraymorph PROPERTIES
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_RPATH "$ORIGIN/../../h5py.libs"
    )
endif()

install(TARGETS arraymorph
    LIBRARY DESTINATION arraymorph/lib
    RUNTIME DESTINATION arraymorph/lib
)
