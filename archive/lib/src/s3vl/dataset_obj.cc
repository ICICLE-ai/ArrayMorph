#include "arraymorph/s3vl/dataset_obj.h"
#include "arraymorph/core/logger.h"
#include "arraymorph/core/utils.h"
#include <algorithm>
#include <assert.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <sstream>
#include <sys/time.h>
#include <thread>

uint64_t transfer_size;
int lambda_num, range_num, total_num;

S3VLDatasetObj::S3VLDatasetObj(const std::string &name, const std::string &uri,
                               hid_t dtype, int ndims,
                               std::vector<hsize_t> &shape,
                               std::vector<hsize_t> &chunk_shape, int chunk_num,
                               const std::string &bucket_name,
                               const CloudClient &client)
    : name(name), uri(uri), dtype(dtype), ndims(ndims), shape(shape),
      chunk_shape(chunk_shape), chunk_num(chunk_num), bucket_name(bucket_name),
      client(client) {
  this->data_size = H5Tget_size(this->dtype);
  Logger::log("Datasize: ", this->data_size);
  // data_size = 4;
  num_per_dim.resize(ndims);
  reduc_per_dim.resize(ndims);
  reduc_per_dim[ndims - 1] = 1;
  for (int i = 0; i < ndims; i++)
    num_per_dim[i] = (shape[i] - 1) / chunk_shape[i] + 1;
  for (int i = ndims - 2; i >= 0; i--)
    reduc_per_dim[i] = reduc_per_dim[i + 1] * num_per_dim[i + 1];
  assert(reduc_per_dim[0] * num_per_dim[0] == chunk_num);

  element_per_chunk = 1;
  for (auto &s : chunk_shape)
    element_per_chunk *= s;
}

std::vector<hsize_t> S3VLDatasetObj::getChunkOffsets(int chunk_idx) {
  std::vector<hsize_t> idx_per_dim(ndims);
  int tmp = chunk_idx;

  for (int i = 0; i < ndims; i++) {
    idx_per_dim[i] = tmp / reduc_per_dim[i] * chunk_shape[i];
    tmp %= reduc_per_dim[i];
  }

  // for (int i = ndims - 1; i >= 0; i--) {
  // 	idx_per_dim[i] = tmp / reduc_per_dim[i] * chunk_shape[i];
  // 	tmp %= reduc_per_dim[i];
  // }
  return idx_per_dim;
}

std::vector<std::vector<hsize_t>>
S3VLDatasetObj::getChunkRanges(int chunk_idx) {
  std::vector<hsize_t> offsets_per_dim = getChunkOffsets(chunk_idx);
  std::vector<std::vector<hsize_t>> re(ndims);
  for (int i = 0; i < ndims; i++)
    re[i] = {offsets_per_dim[i], offsets_per_dim[i] + chunk_shape[i] - 1};
  return re;
}

std::vector<std::vector<hsize_t>>
S3VLDatasetObj::selectionFromSpace(hid_t space_id) {
  hsize_t start[ndims];
  hsize_t end[ndims];
  std::vector<std::vector<hsize_t>> ranges(ndims);
  H5Sget_select_bounds(space_id, start, end);
  for (int i = 0; i < ndims; i++)
    ranges[i] = {start[i], end[i]};
  // if (ndims >= 2)
  // 	swap(ranges[0], ranges[1]);
  return ranges;
}

void processAzure(std::vector<std::shared_ptr<S3VLChunkObj>> &chunk_objs,
                  const std::vector<CPlan> &azure_plans, void *buf,
                  BlobContainerClient *client, const std::string &bucket_name) {
  std::vector<std::future<herr_t>> futures;
  size_t azure_thread_num = THREAD_NUM;
  futures.reserve(azure_thread_num);
  size_t cur_batch_size = 0;

  for (int i = 0; i < azure_plans.size(); i++) {
    const CPlan &p = azure_plans[i];
    if (cur_batch_size != 0 &&
        (cur_batch_size + p.num_requests > azure_thread_num)) {
      while (OperationTracker::getInstance().get() < cur_batch_size)
        ;
      cur_batch_size = 0;
      for (auto &fut : futures)
        fut.wait();
      futures.clear();
    }
    for (auto &s : p.segments) {
      std::vector<std::vector<hsize_t>> mapping;
      for (auto it = s->mapping_start; it != s->mapping_end; ++it)
        mapping.push_back({(*it)[0], (*it)[1], (*it)[2]});
      for (auto &m : mapping)
        m[0] -= s->start_offset;
      auto context = std::make_shared<AsyncReadInput>(buf, mapping);
      futures.push_back(std::async(std::launch::async, Operators::AzureGetRange,
                                   client, chunk_objs[i]->uri, s->start_offset,
                                   s->end_offset, context));
      cur_batch_size++;
      transfer_size += s->end_offset - s->start_offset + 1;
    }
  }
  for (auto &fut : futures)
    fut.wait();
  futures.clear();
}

void processS3(std::vector<std::shared_ptr<S3VLChunkObj>> &chunk_objs,
               const std::vector<CPlan> &s3_plans, void *buf,
               Aws::S3::S3Client *s3_client, const std::string &bucket_name) {
  size_t s3_thread_num = THREAD_NUM;
  size_t cur_batch_size = 0;
  OperationTracker::getInstance().reset();
  for (int i = 0; i < s3_plans.size(); i++) {
    const CPlan &p = s3_plans[i];
    if (cur_batch_size != 0 &&
        (cur_batch_size + p.num_requests > s3_thread_num)) {
      while (OperationTracker::getInstance().get() < cur_batch_size)
        ;
      cur_batch_size = 0;
      OperationTracker::getInstance().reset();
    }
    for (auto &s : p.segments) {
      std::vector<std::vector<hsize_t>> mapping;
      for (auto it = s->mapping_start; it != s->mapping_end; ++it)
        mapping.push_back({(*it)[0], (*it)[1], (*it)[2]});
      for (auto &m : mapping)
        m[0] -= s->start_offset;
      auto context = std::make_shared<AsyncReadInput>(buf, mapping);
      Operators::S3GetByteRangeAsync(s3_client, bucket_name, chunk_objs[i]->uri,
                                     s->start_offset, s->end_offset, context);
      cur_batch_size++;
      transfer_size += s->end_offset - s->start_offset + 1;
    }
  }
  while (OperationTracker::getInstance().get() < cur_batch_size)
    ;
}

herr_t S3VLDatasetObj::read(hid_t mem_space_id, hid_t file_space_id,
                            void *buf) {

  // string lambda_merge_path = getenv("AWS_LAMBDA_MERGE_ACCESS_POINT");
  std::vector<std::vector<hsize_t>> ranges;
  if (file_space_id != H5S_ALL) {
    ranges = selectionFromSpace(file_space_id);
  } else {
    for (int i = 0; i < ndims; i++)
      ranges.push_back({0, shape[i] - 1});
  }

  auto chunk_objs = generateChunks(ranges);
  int num = chunk_objs.size();
  std::vector<std::vector<std::unique_ptr<Segment>>> segments(num);
  std::vector<hsize_t> out_offsets;
  hsize_t out_row_size;
  std::vector<std::list<std::vector<hsize_t>>> global_mapping(num);
  if (mem_space_id == H5S_ALL) {
    // memspace == dataspace
    out_offsets = calSerialOffsets(ranges, shape);
    // out_row_size = ranges[0][1] - ranges[0][0] + 1;
    out_row_size = ranges[ndims - 1][1] - ranges[ndims - 1][0] + 1;
  } else {
    std::vector<std::vector<hsize_t>> out_ranges =
        selectionFromSpace(mem_space_id);
    hsize_t dims_out[ndims];
    H5Sget_simple_extent_dims(mem_space_id, dims_out, NULL);
    std::vector<hsize_t> out_shape(dims_out, dims_out + ndims);
    // if (ndims >= 2)
    // 	swap(out_shape[0], out_shape[1]);
    out_offsets = calSerialOffsets(out_ranges, out_shape);
    // out_row_size = out_ranges[0][1] - out_ranges[0][0] + 1;
    out_row_size = out_ranges[ndims - 1][1] - out_ranges[ndims - 1][0] + 1;
  }
  for (int i = 0; i < num; i++) {
    // hsize_t input_row_size = chunk_objs[i]->ranges[0][1] -
    // chunk_objs[i]->ranges[0][0] + 1;
    hsize_t input_row_size = chunk_objs[i]->ranges[ndims - 1][1] -
                             chunk_objs[i]->ranges[ndims - 1][0] + 1;
    global_mapping[i] = mapHyperslab(chunk_objs[i]->local_offsets,
                                     chunk_objs[i]->global_offsets, out_offsets,
                                     input_row_size, out_row_size, data_size);
    segments[i] = generateSegments(global_mapping[i], chunk_objs[i]->size);
  }

  transfer_size = 0;
  lambda_num = 0;
  range_num = 0;

  // cout << "start plan" << endl;
  struct timeval start_opt, end_opt;
  gettimeofday(&start_opt, NULL);
  std::vector<CPlan> plans;
  plans.reserve(chunk_objs.size());

  for (int i = 0; i < chunk_objs.size(); i++) {
    plans.emplace_back(i, GET, segments[i].size(), std::move(segments[i]));
  }
  gettimeofday(&end_opt, NULL);
  double opt_t = (1000000 * (end_opt.tv_sec - start_opt.tv_sec) +
                  end_opt.tv_usec - start_opt.tv_usec) /
                 1000000.0;
  assert(plans.size() == num);
#ifdef PROFILE_ENABLE
  std::cout << "query processer time: " << opt_t << std::endl;
  std::cout << "chunk num:" << num << std::endl;
#endif
  // cout << "get plans" << endl;
#ifdef LOG_ENABLE
  Logger::log("------ Plans:");
  for (int i = 0; i < num; i++) {
    Logger::log("chunk: ", chunk_objs[i]->uri);
    Logger::log("plan: ", plans[i].qp);
  }
#endif
  assert(num == plans.size());

  // std::sort(plans.begin(), plans.end(), [](const CPlan &a, const CPlan &b) {
  // 	return a.qp < b.qp;
  // });

#ifdef PROFILE_ENABLE
  std::cout << "Plans: " << std::endl;
  std::cout << "total num: " << plans.size() << std::endl;
#endif
  if (SP == AZURE_BLOB) {
    auto azure_client =
        std::get_if<std::unique_ptr<BlobContainerClient>>(&client);
    processAzure(chunk_objs, plans, buf, azure_client->get(), bucket_name);
  } else {
    auto s3_client = std::get_if<std::unique_ptr<Aws::S3::S3Client>>(&client);
    processS3(chunk_objs, plans, buf, s3_client->get(), bucket_name);
  }
#ifdef PROFILE_ENABLE
  std::cout << "transfer_size: " << transfer_size << std::endl;
#endif
  return ARRAYMORPH_SUCCESS;
}

herr_t S3VLDatasetObj::write(hid_t mem_space_id, hid_t file_space_id,
                             const void *buf) {
  std::vector<std::vector<hsize_t>> ranges;
  if (file_space_id != H5S_ALL) {
    ranges = selectionFromSpace(file_space_id);
  } else {
    for (int i = 0; i < ndims; i++)
      ranges.push_back({0, shape[i] - 1});
  }

  auto chunk_objs = generateChunks(ranges);
  int num = chunk_objs.size();
  std::vector<std::list<std::vector<hsize_t>>> mappings(num);
  std::vector<hsize_t> source_offsets;
  hsize_t source_row_size;
  if (mem_space_id == H5S_ALL) {
    // memspace == dataspace
    source_offsets = calSerialOffsets(ranges, shape);
    // source_row_size = ranges[0][1] - ranges[0][0] + 1;
    source_row_size = ranges[ndims - 1][1] - ranges[ndims - 1][0] + 1;
  } else {
    std::vector<std::vector<hsize_t>> source_ranges =
        selectionFromSpace(mem_space_id);
    hsize_t dims_source[ndims];
    H5Sget_simple_extent_dims(mem_space_id, dims_source, NULL);
    std::vector<hsize_t> source_shape(dims_source, dims_source + ndims);
    // if (ndims >= 2)
    // 	swap(source_shape[0], source_shape[1]);
    source_offsets = calSerialOffsets(source_ranges, source_shape);
    // source_row_size = source_ranges[0][1] - source_ranges[0][0] + 1;
    source_row_size =
        source_ranges[ndims - 1][1] - source_ranges[ndims - 1][0] + 1;
  }

  for (int i = 0; i < num; i++) {
    // hsize_t dest_row_size = chunk_objs[i]->ranges[0][1] -
    // chunk_objs[i]->ranges[0][0] + 1;
    hsize_t dest_row_size = chunk_objs[i]->ranges[ndims - 1][1] -
                            chunk_objs[i]->ranges[ndims - 1][0] + 1;
    mappings[i] = mapHyperslab(chunk_objs[i]->local_offsets,
                               chunk_objs[i]->global_offsets, source_offsets,
                               dest_row_size, source_row_size, data_size);
  }

  std::vector<std::future<herr_t>> futures;
  futures.reserve(THREAD_NUM);
  int cur_batch = 0;

  while (cur_batch < num) {
    for (int idx = cur_batch; idx < std::min(num, cur_batch + THREAD_NUM);
         idx++) {
      size_t length = chunk_objs[idx]->size;
      auto upload_buf = std::shared_ptr<char>(new char[length],
                                              std::default_delete<char[]>());
      auto raw_buf = upload_buf.get();
#ifdef DUMMY_WRITE
      memset(raw_buf, 0, length);
#else
      for (auto &m : mappings[idx]) {
        memcpy(raw_buf + m[0], (char *)buf + m[1], m[2]);
      }
#endif
      if (SP == SPlan::AZURE_BLOB) {
        auto azure_client =
            std::get_if<std::unique_ptr<BlobContainerClient>>(&client);
        futures.push_back(std::async(std::launch::async, Operators::AzurePut,
                                     azure_client->get(), chunk_objs[idx]->uri,
                                     upload_buf, length));
      } else {
        auto s3_client =
            std::get_if<std::unique_ptr<Aws::S3::S3Client>>(&client);
        futures.push_back(std::async(std::launch::async, Operators::S3PutBuf,
                                     s3_client->get(), bucket_name,
                                     chunk_objs[idx]->uri, upload_buf, length));
      }
    }
    cur_batch = std::min(num, cur_batch + THREAD_NUM);
    for (auto &fut : futures)
      fut.wait();
    futures.clear();
  }
  return ARRAYMORPH_SUCCESS;
}

std::vector<std::shared_ptr<S3VLChunkObj>>
S3VLDatasetObj::generateChunks(std::vector<std::vector<hsize_t>> ranges) {
  assert(ranges.size() == ndims);
  std::vector<int> accessed_chunks;
  // get accessed chunks
  std::vector<std::vector<hsize_t>> chunk_ranges(ndims);
  for (int i = 0; i < ndims; i++)
    chunk_ranges[i] = {ranges[i][0] / chunk_shape[i],
                       ranges[i][1] / chunk_shape[i]};
  std::vector<hsize_t> chunk_offsets =
      calSerialOffsets(chunk_ranges, num_per_dim);
  // for (int i = 0; i <= chunk_ranges[0][1] - chunk_ranges[0][0]; i++)
  for (int i = 0; i <= chunk_ranges[ndims - 1][1] - chunk_ranges[ndims - 1][0];
       i++)
    for (auto &n : chunk_offsets)
      accessed_chunks.push_back(i + n);

  // iterate accessed chunks and generate queries
  std::vector<std::shared_ptr<S3VLChunkObj>> chunk_objs;
  chunk_objs.reserve(accessed_chunks.size());
  Logger::log("------ # of chunks ", accessed_chunks.size());
  for (auto &c : accessed_chunks) {
    std::vector<hsize_t> offsets = getChunkOffsets(c);
    std::vector<std::vector<hsize_t>> local_ranges(ndims);
    for (int i = 0; i < ndims; i++) {
      hsize_t left = std::max(offsets[i], ranges[i][0]) - offsets[i];
      hsize_t right =
          std::min(offsets[i] + chunk_shape[i] - 1, ranges[i][1]) - offsets[i];
      local_ranges[i] = {left, right};
    }
    std::string chunk_uri = uri + "/" + std::to_string(c);
    // get output serial offsets for each row
    std::vector<std::vector<hsize_t>> global_ranges(ndims);
    std::vector<hsize_t> result_shape(ndims);
    for (int i = 0; i < ndims; i++) {
      global_ranges[i] = {local_ranges[i][0] + offsets[i] - ranges[i][0],
                          local_ranges[i][1] + offsets[i] - ranges[i][0]};
      result_shape[i] = ranges[i][1] - ranges[i][0] + 1;
    }
    std::vector<hsize_t> result_serial_offsets =
        calSerialOffsets(global_ranges, result_shape);

    // S3VLChunkObj *chunk = new S3VLChunkObj(chunk_uri, dtype, local_ranges,
    // chunk_shape, result_serial_offsets);
    auto chunk = std::make_shared<S3VLChunkObj>(
        chunk_uri, dtype, local_ranges, chunk_shape, result_serial_offsets);
    // Logger::log("------ Generate Chunk");
    // Logger::log(chunk->to_string());
    // std::cout << chunk->to_string() << std::endl;
    chunk_objs.push_back(chunk);
  }
  return chunk_objs;
}

// read/write

void S3VLDatasetObj::upload() {
  Logger::log("------ Upload metadata " + uri);
  int length;
  char *buffer = toBuffer(&length);
  std::shared_ptr<char> upload_buf(buffer, std::default_delete<char[]>());
  std::string meta_name = uri + "/meta";
  Result re{std::vector<char>(buffer, buffer + length)};

  if (SP == SPlan::S3) {
    auto s3_client = std::get_if<std::unique_ptr<Aws::S3::S3Client>>(&client);

    if (!s3_client || !s3_client->get()) {
      std::cerr << "S3 client not initialized correctly!" << std::endl;
      return;
    }
    Operators::S3Put(s3_client->get(), bucket_name, meta_name, re);
  } else {
    auto azure_client =
        std::get_if<std::unique_ptr<BlobContainerClient>>(&client);
    if (!azure_client || !azure_client->get()) {
      std::cerr << "Azure client not initialized correctly!" << std::endl;
      return;
    }
    Operators::AzurePut(azure_client->get(), meta_name, upload_buf, length);
  }
}

S3VLDatasetObj *S3VLDatasetObj::getDatasetObj(const CloudClient &client,
                                              const std::string &bucket_name,
                                              const std::string &uri) {
  Result re;
  if (SP == SPlan::S3) {
    auto s3_client = std::get_if<std::unique_ptr<Aws::S3::S3Client>>(&client);

    if (!s3_client || !s3_client->get()) {
      std::cerr << "S3 client not initialized correctly!" << std::endl;
      return nullptr;
    }
    re = Operators::S3Get(s3_client->get(), bucket_name, uri);

  } else {
    auto azure_client =
        std::get_if<std::unique_ptr<BlobContainerClient>>(&client);
    if (!azure_client || !azure_client->get()) {
      std::cerr << "Azure client not initialized correctly!" << std::endl;
      return nullptr;
    }
    re = Operators::AzureGet(azure_client->get(), uri);
  }

  if (re.data.empty()) {
    std::cerr << "Didn't get metadata!" << std::endl;
    return nullptr;
  }
  return S3VLDatasetObj::getDatasetObj(client, bucket_name, re.data);
}

char *S3VLDatasetObj::toBuffer(int *length) {
  int size = 8 + name.size() + uri.size() + sizeof(hid_t) + 4 +
             2 * ndims * sizeof(hsize_t) + 4;
  *length = size;
  char *buffer = new char[size];
  int c = 0;
  int name_length = name.size();
  memcpy(buffer, &name_length, 4);
  c += 4;
  memcpy(buffer + c, name.c_str(), name.size());
  c += name.size();
  int uri_length = uri.size();
  memcpy(buffer + c, &uri_length, 4);
  c += 4;
  memcpy(buffer + c, uri.c_str(), uri.size());
  c += uri.size();
  memcpy(buffer + c, &dtype, sizeof(hid_t));
  c += sizeof(hid_t);
  memcpy(buffer + c, &ndims, 4);
  c += 4;
  memcpy(buffer + c, shape.data(), sizeof(hsize_t) * ndims);
  c += sizeof(hsize_t) * ndims;
  memcpy(buffer + c, chunk_shape.data(), sizeof(hsize_t) * ndims);
  c += sizeof(hsize_t) * ndims;
  memcpy(buffer + c, &chunk_num, 4);
  c += 4;
  return buffer;
}

S3VLDatasetObj *S3VLDatasetObj::getDatasetObj(const CloudClient &client,
                                              const std::string &bucket_name,
                                              std::vector<char> &buffer) {
  std::string name, uri;
  int ndims, chunk_num;
  hid_t dtype;
  int c = 0;

  int name_length;
  memcpy(&name_length, buffer.data() + c, sizeof(int));
  c += sizeof(int);

  name.assign(buffer.data() + c, name_length); // name
  c += name_length;

  int uri_length;
  memcpy(&uri_length, buffer.data() + c, sizeof(int));
  c += sizeof(int);

  uri.assign(buffer.data() + c, uri_length); // uri
  c += uri_length;

  memcpy(&dtype, buffer.data() + c, sizeof(hid_t)); // dtype
  c += sizeof(hid_t);

  memcpy(&ndims, buffer.data() + c, sizeof(int)); // ndims
  c += sizeof(int);

  std::vector<hsize_t> shape(ndims); // shape
  memcpy(shape.data(), (hsize_t *)(buffer.data() + c), sizeof(hsize_t) * ndims);
  c += sizeof(hsize_t) * ndims;

  std::vector<hsize_t> chunk_shape(ndims); // chunk_shape
  memcpy(chunk_shape.data(), (hsize_t *)(buffer.data() + c),
         sizeof(hsize_t) * ndims);
  c += sizeof(hsize_t) * ndims;

  memcpy(&chunk_num, buffer.data() + c, sizeof(int)); // chunk num
  return new S3VLDatasetObj(name, uri, dtype, ndims, shape, chunk_shape,
                            chunk_num, bucket_name, client);
}

std::string S3VLDatasetObj::to_string() {
  std::stringstream ss;
  ss << name << " " << uri << std::endl;
  ss << dtype << " " << ndims << std::endl;
  ss << chunk_num << " " << element_per_chunk << std::endl;
  ;
  for (int i = 0; i < ndims; i++) {
    ss << shape[i] << " ";
  }
  ss << std::endl;
  for (int i = 0; i < ndims; i++) {
    ss << chunk_shape[i] << " ";
  }
  ss << std::endl;
  for (int i = 0; i < ndims; i++) {
    ss << num_per_dim[i] << " ";
  }
  ss << std::endl;
  for (int i = 0; i < ndims; i++) {
    ss << reduc_per_dim[i] << " ";
  }
  ss << std::endl;
  return ss.str();
}
