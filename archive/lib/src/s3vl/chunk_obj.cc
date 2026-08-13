#include "arraymorph/s3vl/chunk_obj.h"
#include <algorithm>
#include <assert.h>
#include <sstream>
S3VLChunkObj::S3VLChunkObj(const std::string &name, hid_t dtype,
                           std::vector<std::vector<hsize_t>> &ranges,
                           const std::vector<hsize_t> &shape,
                           std::vector<hsize_t> &return_offsets)
    : uri(name), dtype(dtype), ranges(ranges), shape(shape),
      global_offsets(return_offsets) {
  this->data_size = H5Tget_size(dtype);
  // this->data_size = 4;
  this->ndims = shape.size();
  assert(this->ndims = ranges.size());

  reduc_per_dim.resize(this->ndims);
  // reduc_per_dim[0] = 1;
  // for (int i = 1; i < this->ndims; i++)
  // 	reduc_per_dim[i] = reduc_per_dim[i - 1] * shape[i - 1];
  reduc_per_dim[this->ndims - 1] = 1;
  for (int i = this->ndims - 2; i >= 0; i--)
    reduc_per_dim[i] = reduc_per_dim[i + 1] * shape[i + 1];
  this->local_offsets = calSerialOffsets(ranges, shape);
  // this->size = reduc_per_dim[ndims - 1] * shape[ndims - 1] * data_size;
  this->size = reduc_per_dim[0] * shape[0] * data_size;
  hsize_t sr = 1;
  for (int i = 0; i < ndims; i++)
    sr *= ranges[i][1] - ranges[i][0] + 1;
  this->required_size = sr * data_size;
}

bool S3VLChunkObj::checkFullWrite() {
  for (int i = 0; i < ndims; i++) {
    if (ranges[i][0] != 0 || ranges[i][1] != shape[i] - 1)
      return false;
  }
  return true;
}

std::string S3VLChunkObj::to_string() {
  std::stringstream ss;
  ss << uri << " " << dtype << " " << ndims << " " << data_size << std::endl;
  for (auto &s : shape)
    ss << s << " ";
  ss << std::endl;
  for (int i = 0; i < ndims; i++) {
    ss << shape[i] << " [" << ranges[i][0] << ", " << ranges[i][1] << "]"
       << std::endl;
  }
  return ss.str();
}
