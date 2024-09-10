#ifndef OPERATORS
#define OPERATORS
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/CSVInput.h>                          // for CSVInput
#include <aws/s3/model/CSVOutput.h>                         // for CSVOutput
#include <aws/s3/model/ExpressionType.h>                    // for Expressio...
#include <aws/s3/model/FileHeaderInfo.h>                    // for FileHeade...
#include <aws/s3/model/InputSerialization.h>                // for InputSeri...
#include <aws/s3/model/OutputSerialization.h>               // for OutputSer...
#include <aws/s3/model/RecordsEvent.h>                      // for RecordsEvent
#include <aws/s3/model/SelectObjectContentHandler.h>        // for SelectObj...
#include <aws/s3/model/StatsEvent.h>                        // for StatsEvent
#include <aws/core/http/Scheme.h>                           // for Scheme
#include <aws/core/client/ClientConfiguration.h>
#include <string>
#include <fstream>
#include <sys/stat.h>
#include <utility>
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <hdf5.h>
#include <mutex>
#include <chrono>
#include "constants.h"
#include "logger.h"

using namespace std;
using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

extern atomic<int> finish;

class AsyncWriteInput:public AsyncCallerContext {
public:
	AsyncWriteInput(const char* buf): buf(buf){}
	const char* buf;
};

class AsyncReadInput: public AsyncCallerContext {
public:
	AsyncReadInput(const void* buf, const vector<vector<hsize_t>> &mapping)
		:buf(buf), mapping(mapping) {}
	const void* buf;
	const vector<vector<hsize_t>> mapping;
};

class Operators {
public:
// S3
	static Result S3Get(S3Client *client, string bucket_name, const Aws::String &object_name);
	static herr_t S3GetAsync(S3Client *client, string bucket_name, const Aws::String &object_name, std::shared_ptr<AsyncCallerContext> input);
	static herr_t S3GetByteRangeAsync(S3Client *client, string bucket_name, const Aws::String &object_name, 
		uint64_t beg, uint64_t end, std::shared_ptr<AsyncCallerContext> input);

    static herr_t S3Put(S3Client *client, string bucket_name, string object_name, Result &re);
    static herr_t S3PutBuf(S3Client *client, string bucket_name, string object_name, char* buf, hsize_t length);
    static herr_t S3PutAsync(S3Client *client, string bucket_name, const Aws::String &object_name, Result &re);
    static herr_t S3Delete(const S3Client *client, string bucket_name, const Aws::String &object_name);
    static void GetAsyncCallback(const Aws::S3::S3Client* s3Client, const Aws::S3::Model::GetObjectRequest& request, Aws::S3::Model::GetObjectOutcome outcome, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);
};

inline herr_t Operators::S3GetByteRangeAsync(S3Client *client, string bucket_name, const Aws::String &object_name, uint64_t beg, uint64_t end,
                                std::shared_ptr<AsyncCallerContext> input)
{
    Logger::log("------ S3getRangeAsync ", object_name);
    GetObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    stringstream ss;
    ss << "bytes=" << beg << '-' << end;
    request.SetRange(ss.str().c_str());
    client->GetObjectAsync(request, GetAsyncCallback, input);
    return SUCCESS;
}

#endif
