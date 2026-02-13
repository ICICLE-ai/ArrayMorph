#ifndef OPERATORS
#define OPERATORS
#include "arraymorph/core/constants.h"
#include "arraymorph/core/logger.h"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h> // for Scheme
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CSVInput.h>  // for CSVInput
#include <aws/s3/model/CSVOutput.h> // for CSVOutput
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ExpressionType.h> // for Expressio...
#include <aws/s3/model/FileHeaderInfo.h> // for FileHeade...
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/InputSerialization.h>  // for InputSeri...
#include <aws/s3/model/OutputSerialization.h> // for OutputSer...
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/RecordsEvent.h>               // for RecordsEvent
#include <aws/s3/model/SelectObjectContentHandler.h> // for SelectObj...
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/s3/model/StatsEvent.h> // for StatsEvent
#include <azure/core/http/policies/policy.hpp>
#include <azure/storage/blobs.hpp> // for Azure blob
#include <chrono>
#include <fstream>
#include <hdf5.h>
#include <iostream>
#include <mutex>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <variant>
#include <vector>
using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Azure::Storage::Blobs;

using CloudClient =
    std::variant<std::monostate, std::unique_ptr<Aws::S3::S3Client>,
                 std::unique_ptr<BlobContainerClient>>;

extern CloudClient global_cloud_client;

class OperationTracker {
public:
  static OperationTracker &getInstance();

  void add();
  int get() const;
  void reset();

private:
  std::atomic<int> finish{0};

  OperationTracker() = default;
  OperationTracker(const OperationTracker &) = delete;
  OperationTracker &operator=(const OperationTracker &) = delete;
};

class AsyncWriteInput : public AsyncCallerContext {
public:
  AsyncWriteInput(const char *buf) : buf(buf) {}
  const char *buf;
};

class AsyncReadInput : public AsyncCallerContext {
public:
  AsyncReadInput(const void *buf,
                 const std::vector<std::vector<hsize_t>> &mapping,
                 const int lambda = 0, const std::string bucket_name = "",
                 const std::string uri = "")
      : buf(buf), mapping(mapping), lambda(lambda), bucket_name(bucket_name),
        uri(uri) {}
  const void *buf;
  const std::vector<std::vector<hsize_t>> mapping;
  const int lambda;
  // for re-issuing GET if lambda fails
  const std::string bucket_name;
  const std::string uri;
};

class Operators {
public:
  // S3
  static Result S3Get(const S3Client *client, const std::string &bucket_name,
                      const Aws::String &object_name);
  static herr_t
  S3GetAsync(const S3Client *client, const std::string &bucket_name,
             const Aws::String &object_name,
             const std::shared_ptr<const AsyncCallerContext> input);
  static herr_t
  S3GetByteRangeAsync(const S3Client *client, const std::string &bucket_name,
                      const Aws::String &object_name, uint64_t beg,
                      uint64_t end,
                      const std::shared_ptr<const AsyncCallerContext> input);
  static herr_t S3Put(const S3Client *client, const std::string &bucket_name,
                      const std::string &object_name, Result &re);
  static herr_t S3PutBuf(const S3Client *client, const std::string &bucket_name,
                         const std::string &object_name,
                         std::shared_ptr<char> buf, hsize_t length);
  static herr_t S3PutAsync(const S3Client *client,
                           const std::string &bucket_name,
                           const Aws::String &object_name, Result &re);
  static herr_t S3Delete(const S3Client *client, const std::string &bucket_name,
                         const Aws::String &object_name);
  static void GetAsyncCallback(
      const Aws::S3::S3Client *s3Client,
      const Aws::S3::Model::GetObjectRequest &request,
      Aws::S3::Model::GetObjectOutcome outcome,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  // Azure
  static Result AzureGet(const BlobContainerClient *client,
                         const std::string &blob_name);
  static herr_t AzurePut(const BlobContainerClient *client,
                         const std::string &blob_name,
                         std::shared_ptr<char> buf, size_t length);
  static herr_t
  AzureGetAndProcess(const BlobContainerClient *client,
                     const std::string &blob_name,
                     const std::shared_ptr<const AsyncCallerContext> context);
  static herr_t
  AzureGetRange(const BlobContainerClient *client, const std::string &blob_name,
                uint64_t beg, uint64_t end,
                const std::shared_ptr<const AsyncCallerContext> context);
};

inline herr_t Operators::S3GetByteRangeAsync(
    const S3Client *client, const std::string &bucket_name,
    const Aws::String &object_name, uint64_t beg, uint64_t end,
    const std::shared_ptr<const AsyncCallerContext> input) {
  Logger::log("------ S3getRangeAsync ", object_name);
  GetObjectRequest request;
  request.SetBucket(bucket_name);
  request.SetKey(object_name);
  std::stringstream ss;
  ss << "bytes=" << beg << '-' << end;
  // std::cout << ss.str() << std::endl;
  request.SetRange(ss.str().c_str());
  client->GetObjectAsync(request, GetAsyncCallback, input);
  return ARRAYMORPH_SUCCESS;
}

#endif
