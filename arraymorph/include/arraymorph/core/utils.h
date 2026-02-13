#ifndef UTILS
#define UTILS
#include "arraymorph/core/constants.h"
#include <assert.h>
#include <hdf5.h>
#include <iostream>
#include <list>
#include <memory>
#include <vector>

class Segment {
public:
  Segment(hsize_t start_offset, hsize_t end_offset, hsize_t required_data_size,
          const std::list<std::vector<hsize_t>>::iterator &mapping_start,
          const std::list<std::vector<hsize_t>>::iterator &mapping_end,
          hsize_t mapping_size);
  Segment(hsize_t start_offset, hsize_t end_offset,
          const std::list<std::vector<hsize_t>>::iterator &mapping_start,
          const std::list<std::vector<hsize_t>>::iterator &mapping_end,
          hsize_t mapping_size);
  Segment(const std::list<std::vector<hsize_t>>::iterator &mapping_start,
          const std::list<std::vector<hsize_t>>::iterator &mapping_end,
          hsize_t mapping_size);
  std::string to_string();
  hsize_t start_offset; // inclusive
  hsize_t end_offset;   // inclusive
  hsize_t required_data_size;
  hsize_t mapping_size;
  std::list<std::vector<hsize_t>>::iterator mapping_start; // inclusive
  std::list<std::vector<hsize_t>>::iterator mapping_end;   // exclusive
};

std::vector<hsize_t> reduceAdd(std::vector<hsize_t> &a,
                               std::vector<hsize_t> &b);

// calculate each row's start position in serial order
std::vector<hsize_t> calSerialOffsets(std::vector<std::vector<hsize_t>> ranges,
                                      std::vector<hsize_t> shape);

// mapping offsets between two hyperslab
std::list<std::vector<hsize_t>>
mapHyperslab(std::vector<hsize_t> &local, std::vector<hsize_t> &global,
             std::vector<hsize_t> &out, hsize_t &input_rsize,
             hsize_t &out_rsize, hsize_t &data_size);

// create Json query for Lambda function
std::string createQuery(hsize_t data_size, int ndims,
                        const std::vector<hsize_t> &shape,
                        std::vector<std::vector<hsize_t>> &ranges);

// generate list of segments for chunk-level query

std::vector<std::unique_ptr<Segment>>
generateSegments(std::list<std::vector<hsize_t>> &mapping, hsize_t chunk_size);

#endif
