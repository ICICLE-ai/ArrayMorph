# ArrayMorph VOL Plugin — Local Garage Setup Changes

This document describes all changes made to get the ArrayMorph HDF5 VOL plugin
working with a local [Garage](https://garagehq.deuxfleurs.fr/) S3-compatible
storage instance running in Docker on macOS (ARM64).

## Directory Tree

Changes made to restructure the code base. CMakeList files were cleaned up as well.

```
.
├── CMakeLists.txt
├── CMakeUserPresets.json // Needed so clangd (LSP) can see packages from conan
├── conanfile.py // Needed for conan to pull packages
├── deactivate.sh
├── include // Moved all header files
│   └── arraymorph
│       ├── core
│       │   ├── constants.h
│       │   ├── logger.h
│       │   ├── operators.h
│       │   └── utils.h
│       └── s3vl
│           ├── chunk_obj.h
│           ├── dataset_callbacks.h
│           ├── dataset_obj.h
│           ├── file_callbacks.h
│           ├── group_callbacks.h
│           ├── initialize.h
│           └── vol_connector.h
├── justfile // Not necessary, but helps with reptetitive cli commands (need to install just).
├── meta.yaml
├── README.md
├── scripts
│   └── extract_perspective.py
└── src // All source files
    ├── CMakeLists.txt
    ├── core
    │   ├── constants.cc
    │   ├── operators.cc
    │   └── utils.cc
    └── s3vl
        ├── chunk_obj.cc
        ├── dataset_callbacks.cc
        ├── dataset_obj.cc
        ├── file_callbacks.cc
        ├── group_callbacks.cc
        └── vol_connector.cc
```

## Environment

- **OS:** macOS (Apple Silicon / ARM64)
- **Garage:** v2.2.0 in Docker, S3 API on `http://localhost:3900`, admin API on `localhost:3903`
- **Python:** h5py with `HDF5_VOL_CONNECTOR=arraymorph`
- **Build:** Conan + CMake + Ninja
- **Variables**: env-example.txt

## Required Changes

These changes are necessary for the plugin to work with a local Garage instance.

### 1. S3 Client Configuration (`src/s3vl/file_callbacks.cpp`)

The S3 client was hardcoded for AWS. Five changes were needed to target a local
Garage instance:

#### a. HTTP Scheme (`file_callbacks.cpp)

```cpp
// Before
s3ClientConfig->scheme = Scheme::HTTPS;

// After
s3ClientConfig->scheme = Scheme::HTTP;
```

**Why:** Garage runs on `http://localhost:3900`. HTTPS causes connection failure.

#### b. Endpoint Override (`file_callbacks.cpp)

```cpp
// Added
const char *endpoint = getenv("AWS_ENDPOINT_URL_S3");
if (endpoint) {
  s3ClientConfig->endpointOverride = endpoint;
}
```

**Why:** Without this, the AWS SDK sends requests to AWS's default S3 endpoint
instead of the local Garage instance. The endpoint is read from the
`AWS_ENDPOINT_URL_S3` environment variable.

#### c. Region Override (`file_callbacks.cpp)

```cpp
// Added
const char *region = getenv("AWS_REGION");
if (region) {
  s3ClientConfig->region = region;
}
```

**Why:** AWS SigV4 includes the region in the request signature. The SDK
defaults to `us-east-1`, but Garage is configured with region `garage`. A
mismatch produces `AccessDenied: Forbidden: Invalid signature`.

#### d. Payload Signing Policy (`file_callbacks.cpp)

```cpp
// Before
Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never

// After
Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always
```

**Why:** Garage requires signed payloads. `Never` skips payload signing, causing
signature validation failures.

#### e. Path-Style Addressing (`file_callbacks.cpp)

```cpp
// Before (last argument = true = virtual-hosted style)
client = std::make_unique<Aws::S3::S3Client>(cred, ..., true);

// After (last argument = false = path-style)
client = std::make_unique<Aws::S3::S3Client>(cred, ..., false);
```

**Why:** Virtual-hosted style puts the bucket name in the hostname
(`bucket.endpoint`). Garage (and most S3-compatible stores) require path-style
(`endpoint/bucket`). With virtual-hosted style, the SDK was interpreting the HDF5
filename as the bucket name.

### 2. AWS SDK Lifecycle (`include/arraymorph/s3vl/initialize.hpp`)

#### a. Global SDK Options (`initialize.hpp:10`)

```cpp
// Added at file scope
static Aws::SDKOptions g_sdk_options;
```

The `SDKOptions` was previously a local variable in `s3VL_initialize_init`. It
must persist so `Aws::ShutdownAPI` can be called during close.

#### b. Added `operators.hpp` Include (`initialize.hpp:4`)

```cpp
#include "arraymorph/core/operators.hpp"
```

Needed for the `global_cloud_client` symbol used in the close function.

#### c. Proper SDK Shutdown (`initialize.hpp:59-65`)

```cpp
// Before
inline herr_t S3VLINITIALIZE::s3VL_initialize_close() {
  Logger::log("------ Close VOL");
  // Aws::ShutdownAPI(opt);
  return ARRAYMORPH_SUCCESS;
}

// After
inline herr_t S3VLINITIALIZE::s3VL_initialize_close() {
  Logger::log("------ Close VOL");
  global_cloud_client = std::monostate{};
  Aws::ShutdownAPI(g_sdk_options);
  return ARRAYMORPH_SUCCESS;
}
```

**Why:** The S3Client must be destroyed *before* `ShutdownAPI` is called.
Otherwise the SDK's internal state is torn down while the client still holds
references to it. Resetting `global_cloud_client` to `monostate` triggers the
`unique_ptr<S3Client>` destructor.

#### d. Terminate Handler (macOS Workaround) (`initialize.hpp:29`)

```cpp
// Added before Aws::InitAPI
std::set_terminate([]() { _exit(0); });
```

**Why:** The AWS CRT (Common Runtime) library registers `atexit` handlers that
conflict with Python's interpreter shutdown on macOS. After all S3 operations
complete successfully, the process crashes with:

```
std::__1::system_error: mutex lock failed: Invalid argument
```

This is a `pthread_mutex_lock` returning `EINVAL` during static destruction —
the mutex has already been destroyed by the time the CRT's cleanup code runs.
The terminate handler catches this and exits cleanly. This is a known class of
issue with the AWS C++ SDK when loaded as a shared library inside another
runtime (Python/h5py).

## Tuning Changes

These values were reduced from production/cloud defaults to work within macOS
thread and resource limits. They are not strictly required for correctness but
prevent `thread constructor failed: Resource temporarily unavailable` crashes.

### `include/arraymorph/core/constants.hpp`

| Variable | Line | Original | Current | Recommended | Notes |
|---|---|---|---|---|---|
| `POOLEXECUTOR` | `constants.hpp:11` | `#define` | `// #define` | Re-enable | Gives the SDK a dedicated thread pool. Only crashed at `poolSize=8192`. Safe at 32. |
| `s3Connections` | `constants.hpp:13` | `256` | `8` | `32` | Max concurrent HTTP connections. Not threads. 256 is fine for AWS but unnecessary locally. |
| `poolSize` | `constants.hpp:16` | `8192` | `16` | `32` | Size of `PooledThreadExecutor` thread pool. 8192 exceeded macOS thread limits. |
| `THREAD_NUM` | `constants.hpp:19` | `256` | `8` | `16` | Batch size for `std::async` chunk upload/download. 256 can hit macOS per-process thread limits on large datasets. |

### Recommended Constants for Local Development

```cpp
#define POOLEXECUTOR

const int s3Connections = 32;
const int poolSize = 32;
const int THREAD_NUM = 16;
```

### Recommended Constants for Production (Linux, Cloud VMs)

```cpp
#define POOLEXECUTOR

const int s3Connections = 256;
const int poolSize = 256;
const int THREAD_NUM = 64;
```

## `.env` Configuration

The following environment variables must be set (loaded via `justfile` with
`set dotenv-load`):

```env
STORAGE_PLATFORM=S3
BUCKET_NAME=playgrounds
AWS_ENDPOINT_URL_S3=garage-s3-endpoint
AWS_S3_ADDRESSING_STYLE="path"
AWS_ACCESS_KEY_ID=garage-access-key
AWS_SECRET_ACCESS_KEY=secret-access-key
AWS_REGION=garage
AWS_USE_PATH_STYLE=true
AWS_USE_TLS=true
AWS_SIGNED_PAYLOADS=true
HDF5_VOL_CONNECTOR=arraymorph
HDF5_PLUGIN_PATH=path-to-libarraymorph
```

## Change Necessity Assessment

Not every change made was strictly necessary.

### Absolutely Necessary (plugin fails without these)

These are the S3 client config changes in `file_callbacks.cpp`:

- **Endpoint override** from `AWS_ENDPOINT_URL_S3` (`file_callbacks.cpp) — without it, requests go to AWS
- **Region override** from `AWS_REGION` (`file_callbacks.cpp) — without it, SigV4 signature is wrong
- **Path-style addressing** (`true` → `false`) (`file_callbacks.cpp) — without it, bucket name is misrouted
- **`PayloadSigningPolicy::Always`** (`file_callbacks.cpp) — without it, Garage rejects unsigned payloads
- **`Scheme::HTTP`** (`file_callbacks.cpp) — without it, can't connect to`<http://localhost:3900`>
- **Garage dead node removal** — without it, writes fail with quorum errors

### Necessary Workaround (operations work, but process crashes on exit)

- **`std::set_terminate([]() { _exit(0); })`** (`initialize.hpp:29`) — masks the AWS CRT mutex crash
  rather than fixing the root cause. It swallows *all* uncaught exceptions, not
  just the CRT one. A more targeted fix would require understanding the CRT's
  internal teardown order. This should be revisited if the plugin is used outside
  of Python/macOS.

### Good Practice but Not Strictly Required

These changes improve correctness/hygiene but the plugin works without them:

- **`global_cloud_client = std::monostate{}`** in close (`initialize.hpp:62`) — clean teardown order,
  but the terminate handler already catches the crash
- **`Aws::ShutdownAPI(g_sdk_options)`** (`initialize.hpp:63`) — proper SDK cleanup, but didn't fix the
  crash by itself
- **`static Aws::SDKOptions g_sdk_options`** (`initialize.hpp:10`) — only needed because of ShutdownAPI
- **`#include "arraymorph/core/operators.hpp"`** (`initialize.hpp:4`) — only
  needed for the monostate reset

### Not Necessary, Can Be Reverted

These were made during investigation and are not required for correctness:

- **`// #define POOLEXECUTOR`** (`constants.hpp:11`) — disabling this was investigative. The crash was
  from `poolSize=8192`, not from the executor itself. Safe to re-enable now that
  `poolSize` is 16.
- **`THREAD_NUM` 256 → 8** (`constants.hpp:19`) — investigative. The test dataset has 1 chunk so
  this value didn't matter. The original thread crash was from `poolSize`, not
  `THREAD_NUM`. Can be set back to a moderate value (16-32).
- **`s3Connections` 256 → 8** (`constants.hpp:13`) — this is HTTP connections, not threads. 256 was
  fine. Changed during investigation of the thread crash.

## Sequence of Errors Encountered

| # | Error | Root Cause | Fix | Location |
|---|-------|------------|-----|----------|
| 1 | `thread constructor failed: Resource temporarily unavailable` | `poolSize=8192` exceeding macOS thread limit | Reduced `poolSize` | `constants.hpp:16` |
| 2 | `NoSuchBucket: Bucket not found: test.h5` | No endpoint override + virtual-hosted addressing | Added `endpointOverride`, set path-style | `file_callbacks.cpp:28-31,40` |
| 3 | `AccessDenied: Forbidden: Invalid signature` | Wrong region + unsigned payloads + HTTPS scheme | Set region from env, `PayloadSigningPolicy::Always`, `Scheme::HTTP` | `file_callbacks.cpp:17,32-35,39` |
| 4 | `ServiceUnavailable: Could not reach quorum` | Dead node in Garage cluster layout | Removed dead node via admin API | Garage admin API |
| 5 | `mutex lock failed: Invalid argument` | AWS CRT static destruction conflict with Python on macOS | `std::set_terminate` workaround + proper SDK shutdown | `initialize.hpp:29,59-65` |
