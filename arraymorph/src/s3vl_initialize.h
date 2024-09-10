#ifndef S3VL_INITIALIZE
#define S3VL_INITIALIZE

#include <aws/core/Aws.h>
#include <hdf5.h>
#include "constants.h"
#include "s3vl_vol_connector.h"
#include <cstdlib>

class S3VLINITIALIZE
{
public:
    static herr_t s3VL_initialize_init(hid_t vipl_id);
    static herr_t s3VL_initialize_close();
};

inline herr_t S3VLINITIALIZE::s3VL_initialize_init(hid_t vipl_id) {
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    Aws::InitAPI(options);
    Logger::log("------ Init VOL");
    return S3_VOL_CONNECTOR_VALUE;
}

inline herr_t S3VLINITIALIZE::s3VL_initialize_close() {
    Logger::log("------ Close VOL");
    return SUCCESS;
}

#endif
