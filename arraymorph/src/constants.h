#ifndef __CONSTANTS__
#define __CONSTANTS__

#include <vector>

// #define LOG_ENABLE
// #define PROFILE_ENABLE
#define SUCCESS 1
#define FAIL -1
#define VOL_ENABLE
#define FILL_VALUE 0
#define PROCESS
#define POOLEXECUTOR

const int s3Connections = 256;
const int requestTimeoutMs = 30000;
const int connectTimeoutMs = 30000;
const int poolSize = 8192;

enum FileFormat {
	binary=0,
	parquet,
	csv
};

typedef struct Result {
	size_t length=0;
	char* data;
} Result;

enum QPlan {
	NONE=-1,
	GET=0
};

#endif















