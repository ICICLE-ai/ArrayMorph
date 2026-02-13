from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeToolchain, CMakeDeps
from conan.tools.gnu import PkgConfigDeps
from conan.tools.layout import basic_layout


class ArrayMorphRecipe(ConanFile):
    name = "ArrayMorph"
    version = "0.2.0"
    settings = "os", "compiler", "build_type", "arch"

    def requirements(self):
        self.requires("aws-sdk-cpp/1.11.692")
        self.requires("azure-sdk-for-cpp/1.16.1")
        # self.requires("azure-storage-cpp/7.5.0")
        self.requires("hdf5/1.14.6")
        self.requires("libcurl/8.17.0")
        self.requires("openssl/3.6.1")

    def configure(self):
        self.options["*"].shared = False
        self.options["aws-sdk-cpp"].s3 = True
        self.options["aws-sdk-cpp"].text_to_speech = False

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self, generator="Ninja")
        tc.generate()
        self.options["azure-sdk-for-cpp"].with_storage_blobs = True
        self.options["azure-sdk-for-cpp"].with_storage_datalake = False
        pc = CMakeDeps(self)
        pc.generate()
