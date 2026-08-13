#include "arraymorph/core/logger.h"
#include "arraymorph/core/operators.h"
#include "arraymorph/s3vl/dataset_callbacks.h"
#include "arraymorph/core/constants.h"
#include <algorithm>
#include <assert.h>
#include <aws/core/utils/threading/Executor.h>
#include <chrono>
#include <cstring>
#include <vector>

const hid_t get_native_type(hid_t tid) {
  hid_t native_list[] = {
      H5T_NATIVE_CHAR,    H5T_NATIVE_SHORT, H5T_NATIVE_INT,
      H5T_NATIVE_LONG,    H5T_NATIVE_LLONG, H5T_NATIVE_UCHAR,
      H5T_NATIVE_USHORT,  H5T_NATIVE_UINT,  H5T_NATIVE_ULONG,
      H5T_NATIVE_ULLONG,  H5T_NATIVE_FLOAT, H5T_NATIVE_DOUBLE,
      H5T_NATIVE_LDOUBLE, H5T_NATIVE_B8,    H5T_NATIVE_B16,
      H5T_NATIVE_B32,     H5T_NATIVE_B64};
  hid_t tmp = H5Tget_native_type(tid, H5T_DIR_DEFAULT);
  for (hid_t t : native_list) {
    if (H5Tequal(t, tmp))
      return t;
  }
  return ARRAYMORPH_FAIL;
}

void *S3VLDatasetCallbacks::S3VL_dataset_create(
    void *obj, const H5VL_loc_params_t *loc_params, const char *name,
    hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id,
    hid_t dxpl_id, void **req) {
  Logger::log("------ Create Dataset: ", name);
  hid_t new_tid = get_native_type(type_id);
  if (new_tid < 0) {
    Logger::log("------ Unsupported data type");
    return NULL;
  }
  S3VLFileObj *file_obj = (S3VLFileObj *)obj;
  std::string uri = file_obj->name + "/" + name;

  int ndims = H5Sget_simple_extent_ndims(space_id);

  hsize_t dims[ndims];
  hsize_t chunk_dims[ndims];
  hsize_t max_dims[ndims];

  H5Sget_simple_extent_dims(space_id, dims, max_dims);

  H5D_layout_t layout = H5Pget_layout(dcpl_id);

  if (layout == H5D_CHUNKED) {
    H5Pget_chunk(dcpl_id, ndims, chunk_dims);
  } else {
    memcpy(chunk_dims, dims, sizeof(hsize_t) * ndims);
  }
  int nchunks = 1;
  for (int i = 0; i < ndims; i++)
    nchunks *= (dims[i] - 1) / chunk_dims[i] + 1;
  std::vector<hsize_t> shape(dims, dims + ndims);
  std::vector<hsize_t> chunk_shape(chunk_dims, chunk_dims + ndims);

  S3VLDatasetObj *ret_obj =
      new S3VLDatasetObj(name, uri, new_tid, ndims, shape, chunk_shape, nchunks,
                         BUCKET_NAME, global_cloud_client);
  ret_obj->is_modified = true;
  Logger::log("------ Create Metadata:");
  Logger::log(ret_obj->to_string());
  return (void *)ret_obj;
}
void *S3VLDatasetCallbacks::S3VL_dataset_open(
    void *obj, const H5VL_loc_params_t *loc_params, const char *name,
    hid_t dapl_id, hid_t dxpl_id, void **req) {
  Logger::log("------ Open dataset ", name);
  // string dset_name(name);
  // cout <<  dset_name << " " << dset_name.size() << endl;
  // if (dset_name == "/") {
  // 	cout << "name is /" << endl;
  // 	dset_name = "test";
  // 	// return NULL;
  // }
  S3VLFileObj *file_obj = (S3VLFileObj *)obj;
  std::string dset_uri = file_obj->name + "/" + name + "/meta";

  S3VLDatasetObj *dset_obj =
      S3VLDatasetObj::getDatasetObj(global_cloud_client, BUCKET_NAME, dset_uri);
  Logger::log("------ Get Metadata:");
  Logger::log(dset_obj->to_string());
  // hid_t type_id = dset_obj->dtype;
  return (void *)dset_obj;
}
herr_t S3VLDatasetCallbacks::S3VL_dataset_read(
    size_t count, void **dset, hid_t *mem_type_id, hid_t *mem_space_id,
    hid_t *file_space_id, hid_t plist_id, void **buf, void **req) {
  S3VLDatasetObj *dset_obj = (S3VLDatasetObj *)(dset[0]);
  Logger::log("------ Read dataset");
  Logger::log("------ Read dataset ", dset_obj->uri);
  // string lower_range = getenv("LOWER_RANGE");
  // string upper_range = getenv("UPPER_RANGE");
  // cout << lower_range << " " << upper_range << endl;
  auto read_start = std::chrono::high_resolution_clock::now();
  std::cout << "read :" << dset_obj->uri << std::endl;
  if (dset_obj->read(*mem_space_id, *file_space_id, buf[0])) {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - read_start;
    std::cout << "VOL read time: " << duration.count() << " seconds"
              << std::endl;
    Logger::log("read successfully");
    return ARRAYMORPH_SUCCESS;
  }
  Logger::log("read failed");
  return ARRAYMORPH_FAIL;
}
herr_t S3VLDatasetCallbacks::S3VL_dataset_write(
    size_t count, void **dset, hid_t *mem_type_id, hid_t *mem_space_id,
    hid_t *file_space_id, hid_t plist_id, const void **buf, void **req) {
  // TODO: update
  S3VLDatasetObj *dset_obj = (S3VLDatasetObj *)(dset[0]);
  Logger::log("------ Write dataset ", dset_obj->uri);
  // vector<int> mem_space = get_range_from_dataspace(mem_space_id);
  // vector<int> file_space = get_range_from_dataspace(file_space_id);

  if (dset_obj->write(*mem_space_id, *file_space_id, buf[0])) {
    Logger::log("write successfully");
    return ARRAYMORPH_SUCCESS;
  }
  Logger::log("write failed");
  return ARRAYMORPH_FAIL;
}

herr_t S3VLDatasetCallbacks::S3VL_dataset_get(void *dset,
                                              H5VL_dataset_get_args_t *args,
                                              hid_t dxpl_id, void **req) {
  S3VLDatasetObj *dset_obj = (S3VLDatasetObj *)dset;
  Logger::log("------ Get Space dataset: ", args->op_type);
  if (args->op_type == H5VL_dataset_get_t::H5VL_DATASET_GET_DCPL) {
    hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    args->args.get_dcpl.dcpl_id = H5Pcopy(dcpl_id);
  } else if (args->op_type == H5VL_dataset_get_t::H5VL_DATASET_GET_SPACE) {
    std::vector<hsize_t> shape = dset_obj->shape;
    // swap(shape[0], shape[1]);
    hid_t space_id = H5Screate_simple(dset_obj->ndims, shape.data(), NULL);
    args->args.get_space.space_id = space_id;
  } else if (args->op_type == H5VL_dataset_get_t::H5VL_DATASET_GET_TYPE) {
    hid_t type_id = H5Tcopy(dset_obj->dtype);
    args->args.get_type.type_id = type_id;
  }
  return ARRAYMORPH_SUCCESS;
}
herr_t S3VLDatasetCallbacks::S3VL_dataset_close(void *dset, hid_t dxpl_id,
                                                void **req) {
  // TODO: update metadata
  Logger::log("------ Close dataset");
  S3VLDatasetObj *dset_obj = (S3VLDatasetObj *)dset;
  if (dset_obj->is_modified)
    dset_obj->upload();

  delete dset_obj;
  return ARRAYMORPH_SUCCESS;
}

void *S3VLDatasetCallbacks::S3VL_obj_open(void *obj,
                                          const H5VL_loc_params_t *loc_params,
                                          H5I_type_t *opened_type,
                                          hid_t dxpl_id, void **req) {
  Logger::log("------ Open object");
  std::string access_name = loc_params->loc_data.loc_by_name.name;
  // cout << "open by name: " << access_name << endl;
  // cout << "open type: " << *opened_type << endl;
  // if (access_name == "/") {
  // 	*opened_type = H5I_type_t::H5I_GROUP;
  // 	vector<string> test = {"1", "2", "3"};
  // 	return new S3VLObj(test, 0);
  // }
  // else {
  *opened_type = H5I_type_t::H5I_DATASET;
  return S3VLDatasetCallbacks::S3VL_dataset_open(
      obj, loc_params, loc_params->loc_data.loc_by_name.name, dxpl_id, dxpl_id,
      req);
  // }
}

herr_t S3VLDatasetCallbacks::S3VL_obj_get(void *obj,
                                          const H5VL_loc_params_t *loc_params,
                                          H5VL_object_get_args_t *args,
                                          hid_t dxpl_id, void **req) {
  Logger::log("------ Object get");
  // cout << args->op_type << endl;
  // cout << loc_params->obj_type << " " << loc_params->type << " " <<
  // loc_params->loc_data.loc_by_name.name << endl;;
  if (args->op_type == H5VL_object_get_t::H5VL_OBJECT_GET_INFO)
    args->args.get_info.oinfo->type = H5O_type_t::H5O_TYPE_GROUP;
  return ARRAYMORPH_SUCCESS;
}
void *S3VLDatasetCallbacks::S3VL_wrap_object(void *obj, H5I_type_t obj_type,
                                             void *wrap_ctx) {
  Logger::log("------ Wrap object");
  return obj;
}

void *S3VLDatasetCallbacks::S3VL_get_object(const void *obj) {
  Logger::log("------ Get object");
  return NULL;
}

herr_t S3VLDatasetCallbacks::S3VL_dataset_specific(
    void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id, void **req) {
  Logger::log("------ Specific dataset: ", args->op_type);
  return ARRAYMORPH_SUCCESS;
}
