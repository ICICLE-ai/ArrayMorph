# 1. Include directories
set(PROJECT_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

# 2. Internal Static Libraries
add_library(utils STATIC core/utils.cc)
target_include_directories(utils PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(utils PUBLIC ${HDF5_LIBRARIES})

add_library(constants STATIC core/constants.cc)
target_include_directories(constants PUBLIC ${PROJECT_INCLUDE_DIRS})

add_library(operators STATIC core/operators.cc)
target_include_directories(operators PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(operators PRIVATE constants ${ALL_DEPS})

add_library(chunk_obj STATIC s3vl/chunk_obj.cc)
target_include_directories(chunk_obj PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(chunk_obj PRIVATE operators utils ${ALL_DEPS})

add_library(dataset_obj STATIC s3vl/dataset_obj.cc)
target_include_directories(dataset_obj PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(dataset_obj PRIVATE chunk_obj ${ALL_DEPS})

add_library(file_callbacks STATIC s3vl/file_callbacks.cc)
target_include_directories(file_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(file_callbacks PRIVATE operators ${ALL_DEPS})

add_library(dataset_callbacks STATIC s3vl/dataset_callbacks.cc)
target_include_directories(dataset_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(dataset_callbacks PRIVATE file_callbacks dataset_obj ${ALL_DEPS})

add_library(group_callbacks STATIC s3vl/group_callbacks.cc)
target_include_directories(group_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(group_callbacks PRIVATE dataset_callbacks ${ALL_DEPS})

# 3. The Final VOL Connector (shared library)
add_library(arraymorph SHARED s3vl/vol_connector.cc)
set_target_properties(arraymorph PROPERTIES PREFIX "lib_")
target_include_directories(arraymorph PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(arraymorph PUBLIC
    group_callbacks
    ${ALL_DEPS}
)

# 4. Install into the Python package's lib/ directory
# scikit-build-core sets CMAKE_INSTALL_PREFIX to the wheel staging area
# so this puts the .so/.dylib at: arraymorph/lib/lib_arraymorph.so
install(TARGETS arraymorph
    LIBRARY DESTINATION arraymorph/lib    # .so (Linux) and .dylib (macOS)
    RUNTIME DESTINATION arraymorph/lib    # .dll (Windows)
)
