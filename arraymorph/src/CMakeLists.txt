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
target_link_libraries(operators PUBLIC constants arraymorph_dependencies)

add_library(chunk_obj STATIC s3vl/chunk_obj.cc)
target_include_directories(chunk_obj PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(chunk_obj PUBLIC operators utils arraymorph_dependencies)

add_library(dataset_obj STATIC s3vl/dataset_obj.cc)
target_include_directories(dataset_obj PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(dataset_obj PUBLIC chunk_obj arraymorph_dependencies)

add_library(file_callbacks STATIC s3vl/file_callbacks.cc)
target_include_directories(file_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(file_callbacks PUBLIC operators arraymorph_dependencies)

add_library(dataset_callbacks STATIC s3vl/dataset_callbacks.cc)
target_include_directories(dataset_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(dataset_callbacks PUBLIC file_callbacks dataset_obj arraymorph_dependencies)

add_library(group_callbacks STATIC s3vl/group_callbacks.cc)
target_include_directories(group_callbacks PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(group_callbacks PUBLIC dataset_callbacks arraymorph_dependencies)

# 3. The Final VOL Connector (shared library)
add_library(array_morph SHARED s3vl/vol_connector.cc)
set_target_properties(array_morph PROPERTIES OUTPUT_NAME "arraymorph")
target_include_directories(array_morph PUBLIC ${PROJECT_INCLUDE_DIRS})
target_link_libraries(array_morph PUBLIC 
    dataset_callbacks 
    file_callbacks 
    group_callbacks 
    arraymorph_dependencies
)

install(TARGETS array_morph LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
