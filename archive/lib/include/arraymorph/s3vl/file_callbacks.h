#ifndef S3VL_FILE_CALLBACKS
#include <hdf5.h>
#include <string>

typedef struct S3VLFileObj {
  std::string name;
} S3VLFileObj;

class S3VLFileCallbacks {
public:
  static void *S3VL_file_create(const char *name, unsigned flags, hid_t fcpl_id,
                                hid_t fapl_id, hid_t dxpl_id, void **req);
  static void *S3VL_file_open(const char *name, unsigned flags, hid_t fapl_id,
                              hid_t dxpl_id, void **req);
  static herr_t S3VL_file_get(void *file, H5VL_file_get_args_t *args,
                              hid_t dxpl_id, void **req);
  static herr_t S3VL_file_close(void *file, hid_t dxpl_id, void **req);
};
#define S3VL_FILE_CALLBACKS
#endif
