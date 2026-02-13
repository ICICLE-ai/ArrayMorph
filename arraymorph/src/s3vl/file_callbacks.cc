#include "arraymorph/s3vl/file_callbacks.h"
#include "arraymorph/core/constants.h"
#include "arraymorph/core/logger.h"
#include "arraymorph/core/operators.h"
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <stdlib.h>
#include <string_view>

CloudClient get_client() {
  Logger::log("Init cloud clients");
  // AWS connection
  CloudClient client;
  if (SP == SPlan::S3) {
    std::string access_key = getenv("AWS_ACCESS_KEY_ID");
    std::string secret_key = getenv("AWS_SECRET_ACCESS_KEY");
    Aws::Auth::AWSCredentials cred(access_key, secret_key);
    std::unique_ptr<Aws::Client::ClientConfiguration> s3ClientConfig =
        std::make_unique<Aws::Client::ClientConfiguration>();

    const char *use_tls =
        getenv("AWS_USE_TLS"); // Is TLS necessary. Does not use it by default.

    // Env var is char pointer by default. Checking to see if value was
    // provided, ('true' or 'false'). If so, cast to std::string_view to avoid
    // heap allocation and check for value.
    s3ClientConfig->scheme = use_tls && std::string_view(use_tls) == "true"
                                 ? Scheme::HTTPS
                                 : Scheme::HTTP;
    const char *endpoint = getenv("AWS_ENDPOINT_URL_S3"); // Custom S3 endpoint
    if (endpoint) {
      s3ClientConfig->endpointOverride = endpoint;
    }

    const char *region =
        getenv("AWS_REGION"); // Region where bucket is located i.e. us-east-1
    if (region) {
      s3ClientConfig->region = region;
    }

    const char *signed_payloads =
        getenv("AWS_SIGNED_PAYLOADS"); // Whether or not to sign each payload.
                                       // Garage requires it to be on. May have
                                       // affect on performance, needs to be
                                       // tested. Off by default
    auto payload_signing_policy =
        *signed_payloads && std::string_view(signed_payloads) == "true"
            ? Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always
            : Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

    const char *path_style =
        getenv("AWS_USE_PATH_STYLE"); // Some S3-compatible stores require path
                                      // styles, 'bucket.endpoint' vs
                                      // 'endpoint/bucket'(with path-style).
    bool use_path_style =
        *path_style && std::string_view(path_style) == "true" ? true : false;

    s3ClientConfig->maxConnections = s3Connections;
    s3ClientConfig->requestTimeoutMs = requestTimeoutMs;
    s3ClientConfig->connectTimeoutMs = connectTimeoutMs;
    s3ClientConfig->retryStrategy =
        std::make_shared<Aws::Client::StandardRetryStrategy>(retries);
#ifdef POOLEXECUTOR
    s3ClientConfig->executor =
        Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("test",
                                                                     poolSize);
#endif
    Logger::log("------ Create Client config: maxConnections=",
                s3ClientConfig->maxConnections);
    client = std::make_unique<Aws::S3::S3Client>(
        cred, std::move(*s3ClientConfig), payload_signing_policy,
        use_path_style);
    s3ClientConfig.reset();
  }
  // Azure connection
  else {
    std::string azure_connection_string =
        getenv("AZURE_STORAGE_CONNECTION_STRING");

    Azure::Core::Http::Policies::RetryOptions retryOptions;
    retryOptions.MaxRetries = retries;
    retryOptions.RetryDelay = std::chrono::milliseconds(200);
    retryOptions.MaxRetryDelay = std::chrono::seconds(2);

    Azure::Storage::Blobs::BlobClientOptions clientOptions;
    clientOptions.Retry = retryOptions;
    client = std::make_unique<BlobContainerClient>(
        BlobContainerClient::CreateFromConnectionString(
            azure_connection_string, BUCKET_NAME, clientOptions));
  }
  return client;
}

void *S3VLFileCallbacks::S3VL_file_create(const char *name, unsigned flags,
                                          hid_t fcpl_id, hid_t fapl_id,
                                          hid_t dxpl_id, void **req) {
  S3VLFileObj *ret_obj = new S3VLFileObj();
  std::string path(name);
  if (path.rfind("./", 0) == 0) {
    path = path.substr(2);
  }
  ret_obj->name = path;
  Logger::log("------ Create File:", path);
  if (std::holds_alternative<std::monostate>(global_cloud_client)) {
    global_cloud_client = get_client();
  }
  return (void *)ret_obj;
}
void *S3VLFileCallbacks::S3VL_file_open(const char *name, unsigned flags,
                                        hid_t fapl_id, hid_t dxpl_id,
                                        void **req) {
  S3VLFileObj *ret_obj = new S3VLFileObj();
  std::string path(name);
  Logger::log("------ Open File:", path);
  if (path.rfind("./", 0) == 0) {
    path = path.substr(2);
  }
  ret_obj->name = path;
  if (std::holds_alternative<std::monostate>(global_cloud_client)) {
    global_cloud_client = get_client();
  }
  return (void *)ret_obj;
}
herr_t S3VLFileCallbacks::S3VL_file_close(void *file, hid_t dxpl_id,
                                          void **req) {
  S3VLFileObj *file_obj = (S3VLFileObj *)file;
  Logger::log("------ Close File: ", file_obj->name);
  delete file_obj;
  return ARRAYMORPH_SUCCESS;
}

herr_t S3VLFileCallbacks::S3VL_file_get(void *file, H5VL_file_get_args_t *args,
                                        hid_t dxpl_id, void **req) {
  Logger::log("------ Get File");
  if (args->op_type == H5VL_file_get_t::H5VL_FILE_GET_FCPL) {
    hid_t fcpl_id = H5Pcreate(H5P_FILE_CREATE);
    args->args.get_fcpl.fcpl_id = H5Pcopy(fcpl_id);
    // args->args.get_fcpl.fcpl_id = 1;
  }
  return ARRAYMORPH_SUCCESS;
}
