#ifndef S3VL_DATASET_OBJ
#define S3VL_DATASET_OBJ
#include "arraymorph/core/constants.h"
#include "arraymorph/core/operators.h"
#include "arraymorph/s3vl/chunk_obj.h"
#include <hdf5.h>
#include <optional>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <variant>
#include <vector>

class S3VLDatasetObj {
public:
  S3VLDatasetObj(const std::string &name, const std::string &uri, hid_t dtype,
                 int ndims, std::vector<hsize_t> &shape,
                 std::vector<hsize_t> &chunk_shape, int chunk_num,
                 const std::string &bucket_name, const CloudClient &client);
  ~S3VLDatasetObj() {};

  static S3VLDatasetObj *getDatasetObj(const CloudClient &client,
                                       const std::string &bucket_name,
                                       const std::string &uri);
  static S3VLDatasetObj *getDatasetObj(const CloudClient &client,
                                       const std::string &bucket_name,
                                       std::vector<char> &buffer);
  char *toBuffer(int *length);
  std::vector<std::shared_ptr<S3VLChunkObj>>
  generateChunks(std::vector<std::vector<hsize_t>> ranges);
  std::string to_string();
  std::vector<hsize_t> getChunkOffsets(int chunk_idx);
  std::vector<std::vector<hsize_t>> getChunkRanges(int chunk_idx);
  std::vector<std::vector<hsize_t>> selectionFromSpace(hid_t space_id);
  // QPlan getQueryPlan(FileFormat format, vector<vector<hsize_t>> ranges);

  void upload();
  herr_t write(hid_t mem_space_id, hid_t file_space_id, const void *buf);
  herr_t read(hid_t mem_space_id, hid_t file_space_id, void *buf);

  const std::string name;
  const std::string uri;
  hid_t dtype;
  const int ndims;
  const std::vector<hsize_t> shape;
  const std::vector<hsize_t> chunk_shape;
  const int chunk_num;
  const std::string bucket_name;

  hsize_t data_size;
  std::vector<hsize_t> num_per_dim;
  std::vector<hsize_t> reduc_per_dim;
  hsize_t element_per_chunk;
  bool is_modified{false};

  const CloudClient &client;
};

#endif
