#ifndef S3VL_GROUP_CALLBACKS
#define S3VL_GROUP_CALLBACKS

#include <string>
#include <hdf5.h>
#include <vector>

using namespace std;

typedef struct S3VLGroupObj {
	vector<string> keys;
} S3VLGroupObj;

class S3VLGroupCallbacks{
public:
	static void* S3VLgroup_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
	static herr_t S3VLgroup_close(void *grp, hid_t dxpl_id, void **req);
	static herr_t S3VLgroup_get(void * obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req); 
	static herr_t S3VLlink_get(void * obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args, hid_t dxpl_id, void **req); 
	static herr_t S3VLlink_specific(void * obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req);
};
#endif
