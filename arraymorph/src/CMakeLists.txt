# Set the template VOL connector name
set (TVC_NAME arraymorph)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 14)
#Load required services/packages: This basic example uses S3.
find_package(AWSSDK REQUIRED COMPONENTS s3)
find_package(CURL REQUIRED)

# Build the template VOL
add_library (${TVC_NAME} SHARED s3vl_vol_connector.cc)
add_library (OPERATORS operators.cc)
add_library (VL_CHUNK_OBJ s3vl_chunk_obj.cc)
add_library (VL_FILE s3vl_file_callbacks.cc)
add_library (VL_DSET s3vl_dataset_callbacks.cc)
add_library (VL_DSET_OBJ s3vl_dataset_obj.cc)
add_library (VL_GROUP s3vl_group_callbacks.cc)
add_library (UTILS utils.cc)

target_link_libraries(OPERATORS PUBLIC ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(VL_CHUNK_OBJ OPERATORS)
target_link_libraries(VL_CHUNK_OBJ UTILS)
target_link_libraries(VL_DSET_OBJ VL_CHUNK_OBJ)
# target_link_libraries(VL_DSET_OBJ OpenMP::OpenMP_CXX)
target_link_libraries(VL_DSET VL_FILE)
target_link_libraries(VL_DSET VL_DSET_OBJ)
target_link_libraries(VL_GROUP VL_DSET)
# target_link_libraries(${TVC_NAME} VL_DSET)
# target_link_libraries(${TVC_NAME} VL_FILE)
target_link_libraries(${TVC_NAME} VL_GROUP)
target_link_libraries(${TVC_NAME} ${HDF5_C_LIBRARIES})

# set_target_properties (${TVC_NAME} PROPERTIES VERSION ${PROJECT_VERSION})
set_target_properties (${TVC_NAME} PROPERTIES SOVERSION 1)
set_target_properties (${TVC_NAME} PROPERTIES PUBLIC_HEADER "${TVC_NAME}.h")
