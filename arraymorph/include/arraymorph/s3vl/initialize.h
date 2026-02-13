#ifndef S3VL_INITIALIZE
#define S3VL_INITIALIZE

#include "arraymorph/core/constants.h"
#include "arraymorph/core/logger.h"
#include "arraymorph/core/operators.h"
#include "arraymorph/s3vl/vol_connector.h"
#include <aws/core/Aws.h>
#include <cstdlib>
#include <exception>
#include <hdf5.h>
#include <optional>
#include <variant>

static Aws::SDKOptions g_sdk_options;

class S3VLINITIALIZE {
public:
  static herr_t s3VL_initialize_init(hid_t vipl_id);
  static herr_t s3VL_initialize_close();
};
std::optional<std::string> getEnv(const char *var) {
  const char *val = std::getenv(var);
  return val ? std::optional<std::string>(val) : std::nullopt;
}

inline herr_t S3VLINITIALIZE::s3VL_initialize_init(hid_t vipl_id) {
  // Aws::SDKOptions options; // Changed to use global sdk options for proper
  // shutdown
  g_sdk_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
  std::set_terminate([]() {
    _exit(0);
  }); // Shutdown conflicts with Python's interpretor shutdown on MacOS.
      // Terminate handler catches this and exits cleanly.
  Aws::InitAPI(g_sdk_options);
  Logger::log("------ Init VOL");
  std::optional<std::string> platform = getEnv("STORAGE_PLATFORM");
  if (platform.has_value()) {
    std::string platform_str = platform.value();
    if (platform_str == "S3")
      Logger::log("------ Using S3");
    else if (platform_str == "Azure") {
      Logger::log("------ Using Azure");
      SP = SPlan::AZURE_BLOB;
    } else {
      Logger::log("------ Unsupported platform");
      return ARRAYMORPH_FAIL;
    }
  } else {
    Logger::log("------ Using default platform S3");
  }
  std::optional<std::string> bucket_name = getEnv("BUCKET_NAME");
  if (bucket_name.has_value()) {
    BUCKET_NAME = bucket_name.value();
    Logger::log("------ Using bucket", BUCKET_NAME);
  } else {
    Logger::log("------ Bucekt not set");
    return ARRAYMORPH_FAIL;
  }
  return S3_VOL_CONNECTOR_VALUE;
}

inline herr_t S3VLINITIALIZE::s3VL_initialize_close() {
  Logger::log("------ Close VOL");
  // Proper SDK shutdown.
  global_cloud_client =
      std::monostate{}; // Reset the client's state to trigger destructor
  Aws::ShutdownAPI(g_sdk_options);
  return ARRAYMORPH_SUCCESS;
}

#endif
