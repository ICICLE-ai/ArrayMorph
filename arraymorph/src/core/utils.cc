#include "arraymorph/core/logger.h"
#include "arraymorph/core/utils.h"
#include <algorithm>
#include <sstream>

Segment::Segment(hsize_t start_offset, hsize_t end_offset,
                 hsize_t required_data_size,
                 const std::list<std::vector<hsize_t>>::iterator &mapping_start,
                 const std::list<std::vector<hsize_t>>::iterator &mapping_end,
                 hsize_t mapping_size)
    : start_offset(start_offset), end_offset(end_offset),
      required_data_size(required_data_size), mapping_start(mapping_start),
      mapping_end(mapping_end), mapping_size(mapping_size) {
  assert(mapping_start != mapping_end);
}

Segment::Segment(hsize_t start_offset, hsize_t end_offset,
                 const std::list<std::vector<hsize_t>>::iterator &mapping_start,
                 const std::list<std::vector<hsize_t>>::iterator &mapping_end,
                 hsize_t mapping_size)
    : start_offset(start_offset), end_offset(end_offset),
      mapping_start(mapping_start), mapping_end(mapping_end),
      mapping_size(mapping_size) {
  assert(mapping_start != mapping_end);
  required_data_size = 0;
  for (auto it = mapping_start; it != mapping_end; ++it)
    required_data_size += (*it)[2];
}

Segment::Segment(const std::list<std::vector<hsize_t>>::iterator &mapping_start,
                 const std::list<std::vector<hsize_t>>::iterator &mapping_end,
                 hsize_t mapping_size)
    : mapping_start(mapping_start), mapping_end(mapping_end),
      mapping_size(mapping_size) {
  assert(mapping_start != mapping_end);
  start_offset = (*mapping_start)[0];
  auto last_mapping = std::prev(mapping_end);
  end_offset = (*last_mapping)[0] + (*last_mapping)[2] - 1;
  required_data_size = 0;
  for (auto it = mapping_start; it != mapping_end; ++it)
    required_data_size += (*it)[2];
}

std::string Segment::to_string() {
  std::stringstream ss;
  ss << "start_offset: " << start_offset << " " << "end_offset: " << end_offset
     << std::endl;
  for (auto it = mapping_start; it != mapping_end; ++it)
    ss << (*it)[0] << " " << (*it)[1] << " " << (*it)[2] << std::endl;
  return ss.str();
}

std::vector<hsize_t> reduceAdd(std::vector<hsize_t> &a,
                               std::vector<hsize_t> &b) {
  std::vector<hsize_t> re;
  re.reserve(a.size() * b.size());
  for (auto &i : a)
    for (auto &j : b)
      re.push_back(i + j);
  return re;
}
// calculate each row's start position in serial order
std::vector<hsize_t> calSerialOffsets(std::vector<std::vector<hsize_t>> ranges,
                                      std::vector<hsize_t> shape) {
  assert(ranges.size() == shape.size());
  int dims = ranges.size();
  std::vector<hsize_t> offsets_per_dim(dims);
  std::vector<std::vector<hsize_t>> offsets(dims - 1);

  offsets_per_dim[dims - 1] = 1;
  for (int i = dims - 2; i >= 0; i--)
    offsets_per_dim[i] = offsets_per_dim[i + 1] * shape[i + 1];

  for (int i = 0; i < dims - 1; i++)
    for (int j = ranges[i][0]; j <= ranges[i][1]; j++)
      offsets[i].push_back(j * offsets_per_dim[i]);
  std::vector<hsize_t> re = {0};
  for (int i = 0; i < dims - 1; i++)
    re = reduceAdd(re, offsets[i]);
  for (auto &o : re)
    o += ranges[dims - 1][0];

  return re;
}

// mapping offsets between two hyperslab
std::list<std::vector<hsize_t>>
mapHyperslab(std::vector<hsize_t> &local, std::vector<hsize_t> &global,
             std::vector<hsize_t> &out, hsize_t &input_rsize,
             hsize_t &out_rsize, hsize_t &data_size) {
  std::list<std::vector<hsize_t>> mapping;

  for (int i = 0; i < local.size(); i++) {
    hsize_t idx = global[i] / out_rsize;
    hsize_t start_offset = out[idx] + global[i] % out_rsize;
    hsize_t remaining = out[idx] + out_rsize - start_offset;
    if (remaining >= input_rsize) {
      mapping.push_back({local[i], start_offset, input_rsize});
    } else {
      mapping.push_back({local[i], start_offset, remaining});
      hsize_t cur = remaining;
      int n = (input_rsize - remaining) / out_rsize;
      for (int j = 1; j <= n; j++) {
        mapping.push_back({local[i] + cur, out[idx + j], out_rsize});
        cur += out_rsize;
      }
      if ((input_rsize - remaining) % out_rsize > 0) {
        mapping.push_back({local[i] + cur, out[idx + n + 1],
                           (input_rsize - remaining) % out_rsize});
      }
    }
  }
  for (auto &m : mapping)
    for (auto &o : m)
      o *= data_size;
  return mapping;
}

std::vector<std::unique_ptr<Segment>>
generateSegments(std::list<std::vector<hsize_t>> &mapping, hsize_t chunk_size) {
  std::vector<std::unique_ptr<Segment>> segments;
  size_t n = mapping.size();
  if (n == 0)
    return segments;
  segments.emplace_back(std::make_unique<Segment>(
      0, chunk_size - 1, mapping.begin(), mapping.end(), mapping.size()));
#ifdef LOG_ENABLE
  for (auto const &s : segments)
    Logger::log(s->to_string());
#endif
  return segments;
}

std::string createQuery(hsize_t data_size, int ndims,
                        const std::vector<hsize_t> &shape,
                        std::vector<std::vector<hsize_t>> &ranges) {
  std::stringstream ss;
  ss << "-" << data_size << "-" << ndims << "-";
  for (auto &s : shape)
    ss << s << "-";
  for (auto &r : ranges)
    ss << r[0] << "-" << r[1] << "-";
  std::string re = ss.str();
  return re.substr(0, re.size() - 1);
}
