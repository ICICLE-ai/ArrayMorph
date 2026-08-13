#ifndef __CONSTANTS__
#define __CONSTANTS__

#include <string>
#include <vector>

// #define LOG_ENABLE
// #define PROFILE_ENABLE
#define ARRAYMORPH_SUCCESS 1
#define ARRAYMORPH_FAIL -1
#define FILL_VALUE 0
#define PROCESS
#define POOLEXECUTOR

const int s3Connections = 256;
const int requestTimeoutMs = 30000;
const int connectTimeoutMs = 30000;
const int poolSize = 8192;
const int retries = 3;

const int THREAD_NUM = 256;

extern std::string BUCKET_NAME;

typedef struct Result {
  std::vector<char> data;
} Result;

enum QPlan { NONE = -1, GET = 0 };

enum SPlan { S3 = 0, GOOGLE, AZURE_BLOB };

extern SPlan SP;

#endif
