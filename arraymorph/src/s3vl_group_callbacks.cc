#include "s3vl_group_callbacks.h"
#include "s3vl_dataset_callbacks.h"
#include <aws/core/utils/threading/Executor.h>
#include "operators.h"
#include "logger.h"
#include <cstring>

herr_t S3VLGroupCallbacks::S3VLgroup_get(void * obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req) {
    Logger::log("------ Get Group");
    return SUCCESS;
}

void* S3VLGroupCallbacks::S3VLgroup_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id,
void **req) {
    /*
    This function is a simple way to list the dataset names using h5ls
    */
    Logger::log("------ Open Group");
    Logger::log("name: ", name);
    if (strcmp(name, "/") == 0) {
        S3VLFileObj *f_obj = (S3VLFileObj*)obj;
        string keys_url = f_obj->name + "/keys";
        string bucket_name = getenv("BUCKET_NAME");
        Result keys;
        // aws connection
        Aws::S3::S3Client *client;
        string access_key = getenv("AWS_ACCESS_KEY_ID");
        string secret_key = getenv("AWS_SECRET_ACCESS_KEY");
        Aws::Auth::AWSCredentials cred(access_key, secret_key);
        if (s3Configured == false) {
            s3Configured = true;
            s3ClientConfig = new Aws::Client::ClientConfiguration();
            s3ClientConfig->scheme = Scheme::HTTP;
            s3ClientConfig->maxConnections = s3Connections;
            s3ClientConfig->requestTimeoutMs = requestTimeoutMs;
            s3ClientConfig->connectTimeoutMs = connectTimeoutMs;
#ifdef POOLEXECUTOR
            s3ClientConfig->executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("test", poolSize);
#endif
            Logger::log("------ Create Client config: maxConnections=", s3ClientConfig->maxConnections);
        }
        client = new S3Client(cred, *s3ClientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
        keys = Operators::S3Get(client, bucket_name, keys_url);
        // list dsets
        cout << "datasets:" << endl;
        cout.write(keys.data, keys.length);
        delete client;
    }
    return (void*)obj;
}

herr_t S3VLGroupCallbacks::S3VLgroup_close(void *grp, hid_t dxpl_id, void **req) {
    Logger::log("------ Close Group");
    return SUCCESS;
}		

herr_t S3VLGroupCallbacks::S3VLlink_get(void * obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args, hid_t dxpl_id, void **req) {
    Logger::log("------ Get Link");
    return SUCCESS;
}


herr_t S3VLGroupCallbacks::S3VLlink_specific(void * obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req) {
    Logger::log("------ Specific Link");
    return SUCCESS;
}
