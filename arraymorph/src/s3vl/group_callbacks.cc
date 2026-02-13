#include "arraymorph/s3vl/group_callbacks.h"
#include "arraymorph/core/logger.h"
#include "arraymorph/core/operators.h"
#include "arraymorph/s3vl/dataset_callbacks.h"
#include <aws/core/utils/threading/Executor.h>
#include <cstring>

herr_t S3VLGroupCallbacks::S3VLgroup_get(void *obj, H5VL_group_get_args_t *args,
                                         hid_t dxpl_id, void **req) {
  Logger::log("------ Get Group");
  // cout << "is NULL: " << (obj == NULL) << endl;
  // if (args->args.get_info.loc_params.obj_type == H5I_type_t::H5I_GROUP ) {
  //     S3VLObj *get_obj = (S3VLObj*)obj;
  //     cout << "obj info: " << get_obj->idx << " " << get_obj->keys[1] <<
  //     endl;
  // }
  // cout << "get type: " << args->op_type << endl;
  // cout << "get para: " << args->args.get_info.loc_params.obj_type << " " <<
  // args->args.get_info.loc_params.type << endl; if (args->op_type ==
  // H5VL_group_get_t::H5VL_GROUP_GET_INFO) {
  //     args->args.get_info.loc_params.type =
  //     H5VL_loc_type_t::H5VL_OBJECT_BY_IDX; auto ginfo =
  //     args->args.get_info.ginfo; ginfo->nlinks = 3; ginfo->mounted = true;
  // }
  // else if (args->op_type == H5VL_group_get_t::H5VL_GROUP_GET_GCPL) {
  //     hid_t gcpl_id = H5Pcreate(H5P_GROUP_CREATE);
  // 	args->args.get_gcpl.gcpl_id = H5Pcopy(gcpl_id);
  // }
  return ARRAYMORPH_SUCCESS;
}

void *S3VLGroupCallbacks::S3VLgroup_open(void *obj,
                                         const H5VL_loc_params_t *loc_params,
                                         const char *name, hid_t gapl_id,
                                         hid_t dxpl_id, void **req) {
  /*
  TODO: This function is a simple way to list the dataset names using h5ls
  */
  Logger::log("------ Open Group");
  Logger::log("name: ", name);
  //     if (strcmp(name, "/") == 0) {
  //         S3VLFileObj *f_obj = (S3VLFileObj*)obj;
  //         std::string keys_url = f_obj->name + "/keys";
  //         std::string bucket_name = getenv("BUCKET_NAME");
  //         Result keys;
  //         // aws connection
  //         if (SP == SPlan::S3) {
  //             std::string access_key = getenv("AWS_ACCESS_KEY_ID");
  //             std::string secret_key = getenv("AWS_SECRET_ACCESS_KEY");
  //             Aws::Auth::AWSCredentials cred(access_key, secret_key);
  //             if (s3Configured == false) {
  //                 s3Configured = true;
  //                 s3ClientConfig = new Aws::Client::ClientConfiguration();
  //                 s3ClientConfig->scheme = Scheme::HTTP;
  //                 s3ClientConfig->maxConnections = s3Connections;
  //                 s3ClientConfig->requestTimeoutMs = requestTimeoutMs;
  //                 s3ClientConfig->connectTimeoutMs = connectTimeoutMs;
  // #ifdef POOLEXECUTOR
  //                 s3ClientConfig->executor =
  //                 Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("test",
  //                 poolSize);
  // #endif
  //                 Logger::log("------ Create Client config: maxConnections=",
  //                 s3ClientConfig->maxConnections);
  //             }
  //             std::unique_ptr<Aws::S3::S3Client> client =
  //             std::make_unique<Aws::S3::S3Client>(cred, *s3ClientConfig);
  //             keys = Operators::S3Get(client.get(), bucket_name, keys_url);
  //             // list dsets
  //             std::cout << "datasets:" << std::endl;
  //             std::cout.write(keys.data, keys.length);
  //         }
  //     }
  return (void *)obj;
}

herr_t S3VLGroupCallbacks::S3VLgroup_close(void *grp, hid_t dxpl_id,
                                           void **req) {
  Logger::log("------ Close Group");
  S3VLFileObj *f_obj = (S3VLFileObj *)grp;
  // free(f_obj);
  return ARRAYMORPH_SUCCESS;
}

herr_t S3VLGroupCallbacks::S3VLlink_get(void *obj,
                                        const H5VL_loc_params_t *loc_params,
                                        H5VL_link_get_args_t *args,
                                        hid_t dxpl_id, void **req) {
  Logger::log("------ Get Link");
  // cout << "get type: " << args->op_type << endl;
  // cout << "get para: " << args->args.get_info.loc_params.obj_type << " " <<
  // args->args.get_info.loc_params.type << endl;
  return ARRAYMORPH_SUCCESS;
}

herr_t S3VLGroupCallbacks::S3VLlink_specific(
    void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req) {
  Logger::log("------ Specific Link");
  // S3VLGroupObj *get_obj = (S3VLGroupObj*)obj;
  // cout << "obj info: " << get_obj->keys.size() << " " << get_obj->keys[1] <<
  // endl;

  // cout << "get type: " << args->op_type << endl;
  // // cout << "get para: " << args->args.get_info.loc_params.obj_type << " "
  // << args->args.get_info.loc_params.type << endl; if (args->op_type ==
  // H5VL_link_specific_t::H5VL_LINK_ITER) {
  //     auto it_args = args->args.iterate;
  //     cout << it_args.recursive << endl;
  //     cout << it_args.op << endl;
  //     // cout << *(it_args.idx_p) << endl;
  //     // cout << loc_params->loc_data.loc_by_name.name << endl;
  // }
  return ARRAYMORPH_SUCCESS;
}
