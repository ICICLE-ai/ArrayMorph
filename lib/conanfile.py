from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeToolchain, CMakeDeps


class ArrayMorphRecipe(ConanFile):
    name = "ArrayMorph"
    version = "0.2.0"

    settings = "os", "compiler", "build_type", "arch"

    requires = (
        "aws-sdk-cpp/1.11.692",
        "azure-sdk-for-cpp/1.16.1",
        "hdf5/1.14.6",  # headers only in practice; runtime comes from h5py
        "libcurl/8.17.0",
        "openssl/3.6.1",
    )

    default_options = {
        # We do NOT want to ship Conan's HDF5 runtime in the wheel.
        # Keeping this static reduces accidental runtime coupling.
        "hdf5/*:shared": False,

        # Keep these static too so we don't need extra wheel bundling.
        "aws-sdk-cpp/*:shared": False,
        "azure-sdk-for-cpp/*:shared": False,
        "libcurl/*:shared": True,
        "openssl/*:shared": True,

        # AWS: only S3
        "aws-sdk-cpp/*:s3": True,
        "aws-sdk-cpp/*:monitoring": False,
        "aws-sdk-cpp/*:transfer": False,
        "aws-sdk-cpp/*:queues": False,
        "aws-sdk-cpp/*:identity-management": False,
        "aws-sdk-cpp/*:access-management": False,
        "aws-sdk-cpp/*:s3-encryption": False,
        "aws-sdk-cpp/*:text-to-speech": False,

        # Azure: only Blob
        "azure-sdk-for-cpp/*:with_storage_blobs": True,
        "azure-sdk-for-cpp/*:with_storage_datalake": False,
    }

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self, generator="Ninja")
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()
