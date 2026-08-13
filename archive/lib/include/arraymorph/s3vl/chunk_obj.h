#ifndef S3VL_CHUNK_OBJ
#define S3VL_CHUNK_OBJ
#include "arraymorph/core/constants.h"
#include "arraymorph/core/logger.h"
#include "arraymorph/core/operators.h"
#include "arraymorph/core/utils.h"
#include <hdf5.h>
#include <iostream>
#include <set>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <vector>

typedef struct CPlan {
  int chunk_id;
  QPlan qp;
  size_t num_requests;
  std::vector<std::unique_ptr<Segment>> segments;
  std::string lambda_query = "";

  CPlan(int id, QPlan q, size_t reqs, std::vector<std::unique_ptr<Segment>> &&s)
      : chunk_id(id), qp(q), num_requests(reqs), segments(std::move(s)) {}

  CPlan(const CPlan &) = delete;
  CPlan &operator=(const CPlan &) = delete;

  CPlan(CPlan &&) = default;
  CPlan &operator=(CPlan &&) = default;

} CPlan;

class S3VLChunkObj {
public:
  S3VLChunkObj(const std::string &uri, hid_t dtype,
               std::vector<std::vector<hsize_t>> &ranges,
               const std::vector<hsize_t> &shape,
               std::vector<hsize_t> &return_offsets);
  ~S3VLChunkObj() {};

  std::string to_string();
  bool checkFullWrite();

  const std::string uri;
  hid_t dtype;
  std::vector<std::vector<hsize_t>> ranges;
  const std::vector<hsize_t> shape;
  std::vector<hsize_t> reduc_per_dim;
  int ndims;

  int data_size;
  hsize_t size;
  hsize_t required_size;
  std::vector<hsize_t> global_offsets;
  std::vector<hsize_t> local_offsets;
};

#endif
