#ifndef S3VL_DATASET_CALLBACKS

#include "arraymorph/core/operators.h"
#include "arraymorph/s3vl/dataset_obj.h"
#include "arraymorph/s3vl/file_callbacks.h"
#include <hdf5.h>
#include <string>

class S3VLDatasetCallbacks {
public:
  static void *S3VL_dataset_create(void *obj,
                                   const H5VL_loc_params_t *loc_params,
                                   const char *name, hid_t lcpl_id,
                                   hid_t type_id, hid_t space_id, hid_t dcpl_id,
                                   hid_t dapl_id, hid_t dxpl_id, void **req);
  static void *S3VL_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
                                 const char *name, hid_t dapl_id, hid_t dxpl_id,
                                 void **req);
  static void *S3VL_obj_open(void *obj, const H5VL_loc_params_t *loc_params,
                             H5I_type_t *opened_type, hid_t dxpl_id,
                             void **req);
  static herr_t S3VL_obj_get(void *obj, const H5VL_loc_params_t *loc_params,
                             H5VL_object_get_args_t *args, hid_t dxpl_id,
                             void **req);
  static void *S3VL_wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx);
  static void *S3VL_get_object(const void *obj);
  static herr_t S3VL_dataset_read(size_t count, void **dset, hid_t *mem_type_id,
                                  hid_t *mem_space_id, hid_t *file_space_id,
                                  hid_t plist_id, void **buf, void **req);
  static herr_t S3VL_dataset_write(size_t count, void **dset,
                                   hid_t *mem_type_id, hid_t *mem_space_id,
                                   hid_t *file_space_id, hid_t plist_id,
                                   const void **buf, void **req);
  static herr_t S3VL_dataset_get(void *dset, H5VL_dataset_get_args_t *args,
                                 hid_t dxpl_id, void **req);
  static herr_t S3VL_dataset_close(void *dset, hid_t dxpl_id, void **req);
  static herr_t S3VL_dataset_specific(void *obj,
                                      H5VL_dataset_specific_args_t *args,
                                      hid_t dxpl_id, void **req);
};
#define S3VL_DATASET_CALLBACKS
#endif
