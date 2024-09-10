#include "operators.h"
#include "logger.h"
#include <assert.h>
#include <time.h>
#include <chrono>

void PutAsyncCallback(const Aws::S3::S3Client* s3Client, 
    const Aws::S3::Model::PutObjectRequest& request, 
    const Aws::S3::Model::PutObjectOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
    if (outcome.IsSuccess()) {
        Logger::log("write async successfully: ", request.GetKey());
        finish++;
    }
    else {
        Logger::log("write async failed: ", request.GetKey());
    }
    const std::shared_ptr<const AsyncWriteInput> input = static_pointer_cast<const AsyncWriteInput>(context);
    delete[] input->buf;
}


void Operators::GetAsyncCallback(const Aws::S3::S3Client* s3Client, 
    const Aws::S3::Model::GetObjectRequest& request, 
    Aws::S3::Model::GetObjectOutcome outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
    const std::shared_ptr<const AsyncReadInput> input = static_pointer_cast<const AsyncReadInput>(context);
    if (outcome.IsSuccess()) {
        auto& file = outcome.GetResultWithOwnership().GetBody();
        file.seekg(0, file.end);
        size_t length = file.tellg();
        file.seekg(0, file.beg);
      
#ifdef PROCESS
        if (length < 1024 * 1024 * 1024) {
            char* buf = new char[length];
            file.read(buf, length);
            for (auto &m: input->mapping) {
                memcpy((char*)input->buf + m[1], buf + m[0], m[2]);
            }
            delete[] buf;
        }
        else {
            for (auto &m: input->mapping) {
                file.seekg(m[0], file.beg);
                file.read((char*)input->buf + m[1], m[2]);
            }
        }
#endif
    } else {
        auto err = outcome.GetError();
        cout << request.GetKey() << endl;
        std::cout << "Error: GetObject: " <<
            err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    finish++;
    Logger::log("read async successfully: ", request.GetKey());

}


herr_t Operators::S3GetAsync(S3Client *client, string bucket_name, const Aws::String &object_name,
                    std::shared_ptr<AsyncCallerContext> input)
{

    Logger::log("------ S3getAsync ", object_name);
    GetObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    auto handler = [&](const HttpRequest* http_req,
                        HttpResponse* http_resp,
                        long long) {
        if (http_resp->HasHeader("triggered"))
            return;
        http_resp->AddHeader("triggered", "Yes");
        auto headers = http_resp->GetHeaders();
        cout << "rid: " << headers["x-amz-request-id"] << endl;
    };
    client->GetObjectAsync(request, GetAsyncCallback, input);
    return SUCCESS;
}

Result Operators::S3Get(S3Client *client, string bucket_name, const Aws::String &object_name)
{
    Result re;

    Logger::log("------ S3get ", object_name);
    Logger::log("bucket_name ", bucket_name);
    GetObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);

    auto outcome = client->GetObject(request);
    if (outcome.IsSuccess()) {
        auto& file = outcome.GetResultWithOwnership().GetBody();
        file.seekg(0, file.end);
        size_t length = file.tellg();
        file.seekg(0, file.beg);
        char* buf = new char[length];
        file.read(buf, length);
        re.length = length;
        re.data = buf;
    } else {
        auto err = outcome.GetError();
        std::cout << "Error: GetObject: " <<
            err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    return re;
}

herr_t Operators::S3Delete(const S3Client *client, string bucket_name, const Aws::String &object_name) {
    Logger::log("------ S3Delete ", object_name);
    DeleteObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);

    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess())
    {
        auto err = outcome.GetError();
        std::cout << "Error: DeleteObject: " <<
            err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

herr_t Operators::S3PutBuf(S3Client *client, string bucket_name, string object_name, char* buf, hsize_t length)
{
    Logger::log("------ S3Put ", object_name);
    PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    
    auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    input_data->write(buf, length);
    request.SetBody(input_data);

    auto outcome = client->PutObject(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        std::cerr << "ERROR: PutObject: " << 
            err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        return FAIL;
    }
    delete[] buf;
    return SUCCESS;
}

herr_t Operators::S3Put(S3Client *client, string bucket_name, string object_name, Result &re)
{
    Logger::log("------ S3Put ", object_name);
    PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    
    auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    input_data->write(re.data, re.length);
    request.SetBody(input_data);

    auto outcome = client->PutObject(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        std::cerr << "ERROR: PutObject: " << 
            err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        return FAIL;
    }
    delete[] re.data;
    return SUCCESS;
}

herr_t Operators::S3PutAsync(S3Client *client, string bucket_name, const Aws::String &object_name, Result &re)
{
    Logger::log("------ S3PutAsync ", object_name);
    PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    
    auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    input_data->write(re.data, re.length);
    shared_ptr<AsyncCallerContext> context(new AsyncWriteInput(re.data));

    request.SetBody(input_data);
    client->PutObjectAsync(request, PutAsyncCallback, context);
    return SUCCESS;
}
