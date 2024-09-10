#include "s3vl_dataset_obj.h"
#include "utils.h"
#include "logger.h"
#include <algorithm>
#include <sstream>
#include <assert.h>
#include <fstream>
#include <cstring>
#include <chrono>
#include <thread>
#include <sys/time.h>

atomic<int> finish;
uint64_t transfer_size;
int s3_req_num, get_num;

S3VLDatasetObj::S3VLDatasetObj(string name, string uri, hid_t dtype, int ndims, vector<hsize_t> shape, vector<hsize_t> chunk_shape, 
		vector<FileFormat> formats, vector<int> n_bits, int chunk_num, S3Client *client, string bucket_name 
		) {

	this->name = name;
	this->uri = uri;
	this->dtype = dtype;
	this->ndims = ndims;
	this->shape = shape;
	this->chunk_shape = chunk_shape;
	this->data_size = H5Tget_size(this->dtype);
	this->formats = formats;
	this->chunk_num = chunk_num;
	assert(chunk_num == formats.size());
	vector<hsize_t> num_per_dim(ndims);
	vector<hsize_t> reduc_per_dim(ndims);
	reduc_per_dim[ndims - 1] = 1;
	for (int i = 0; i < ndims; i++)
		num_per_dim[i] = (this->shape[i] - 1) / this->chunk_shape[i] + 1;
	for (int i = ndims - 2; i >= 0; i--)
		reduc_per_dim[i] = reduc_per_dim[i + 1] * num_per_dim[i + 1];
	this->num_per_dim = num_per_dim;
	this->reduc_per_dim = reduc_per_dim;
	this->n_bits = n_bits;
	assert(reduc_per_dim[0] * num_per_dim[0] == chunk_num);

	hsize_t element_per_chunk = 1;
	for (auto &s: this->chunk_shape)
		element_per_chunk *= s;
	this->element_per_chunk = element_per_chunk;
    this->is_modified = false;

    // AWS connection
    this->bucket_name = bucket_name;
    this->s3_client = client;
}

vector<hsize_t> S3VLDatasetObj::getChunkOffsets(int chunk_idx) {
	vector<hsize_t> idx_per_dim(ndims);
	int tmp = chunk_idx;

	for (int i = 0; i < ndims; i++) {
		idx_per_dim[i] = tmp / reduc_per_dim[i] * chunk_shape[i];
		tmp %= reduc_per_dim[i];
	}

	return idx_per_dim;
}

vector<vector<hsize_t>> S3VLDatasetObj::getChunkRanges(int chunk_idx) {
	vector<hsize_t> offsets_per_dim = getChunkOffsets(chunk_idx);
	vector<vector<hsize_t>> re(ndims);
	for (int i = 0; i < ndims; i++)
		re[i] = {offsets_per_dim[i], offsets_per_dim[i] + chunk_shape[i] - 1};
	return re;
}

vector<vector<hsize_t>> S3VLDatasetObj::selectionFromSpace(hid_t space_id) {
	hsize_t start[ndims];
	hsize_t end[ndims];
	vector<vector<hsize_t>> ranges(ndims);
	H5Sget_select_bounds(space_id, start, end);
	for (int i = 0; i < ndims; i++)
		ranges[i] = {start[i], end[i]};
	return ranges;
}

herr_t S3VLDatasetObj::read(hid_t mem_space_id, hid_t file_space_id, void* buf) {
    vector<vector<hsize_t>> ranges;
	if (file_space_id != H5S_ALL) {
		ranges = selectionFromSpace(file_space_id);
	}
	else {
		for (int i = 0;i < ndims; i++)
			ranges.push_back({0, shape[i] - 1});
	}
	
	vector<S3VLChunkObj*> chunk_objs = generateChunks(ranges);
	int num = chunk_objs.size();
	vector<vector<vector<hsize_t>>> mappings(num);

	vector<hsize_t> out_offsets;
	hsize_t out_row_size;
	if (mem_space_id == H5S_ALL) {
		out_offsets = calSerialOffsets(ranges, shape);
		out_row_size = ranges[ndims - 1][1] - ranges[ndims - 1][0] + 1;
	}
	else {
		vector<vector<hsize_t>> out_ranges = selectionFromSpace(mem_space_id);
		hsize_t dims_out[ndims];
		H5Sget_simple_extent_dims(mem_space_id, dims_out, NULL);
		vector<hsize_t> out_shape(dims_out, dims_out + ndims);
		out_offsets = calSerialOffsets(out_ranges, out_shape);
		out_row_size = out_ranges[ndims - 1][1] - out_ranges[ndims - 1][0] + 1;
	}
	for (int i = 0; i < num; i++) {
		hsize_t input_row_size = chunk_objs[i]->ranges[ndims - 1][1] - chunk_objs[i]->ranges[ndims - 1][0] + 1;
		mappings[i] = mapHyperslab(chunk_objs[i]->local_offsets, chunk_objs[i]->global_offsets, out_offsets, 
						input_row_size, out_row_size, data_size);
	}
	finish = 0;
    transfer_size = 0;
    s3_req_num = 0;
    get_num = 0;

	vector<CPlan> plans(num);
	for (int i = 0; i < num; i++) {
		plans[i] = CPlan{i, QPlan::GET};
	}
#ifdef LOG_ENABLE
    Logger::log("------ Plans:");
    for (int i = 0; i < num; i++) {
    	Logger::log("chunk: ", chunk_objs[i]->uri);
    	Logger::log("plan: ", plans[i].qp);
    }
#endif
    vector<CPlan> s3_plans;

    for (int i = 0; i < num; i++) {
    	CPlan p = plans[i];
		assert(p == QPlan::GET);
		get_num++;
		s3_plans.push_back(p);
	}
#ifdef PROFILE_ENABLE
    cout << "Plans: " << endl;
    cout << "GET: " << get_num << endl;
#endif
    // process S3 async
    for (auto &p: s3_plans) {
    	int i = p.chunk_id;
		shared_ptr<AsyncCallerContext> context(new AsyncReadInput(buf, mappings[i]));
		transfer_size += chunk_objs[i]->size;
		Operators::S3GetAsync(s3_client, bucket_name, chunk_objs[i]->uri, context);
		s3_req_num++;
    }

	while (finish < s3_req_num);
#ifdef PROFILE_ENABLE
	cout << "transfer_size: " << transfer_size << endl;
	cout << "s3_req_num: " << s3_req_num << endl;
#endif
	return SUCCESS;
}

herr_t S3VLDatasetObj::write(hid_t mem_space_id, hid_t file_space_id, const void* buf) {
	vector<vector<hsize_t>> ranges;
	if (file_space_id != H5S_ALL) {
		ranges = selectionFromSpace(file_space_id);
	}
	else {
		for (int i = 0;i < ndims; i++)
			ranges.push_back({0, shape[i] - 1});
	}
	
	vector<S3VLChunkObj*> chunk_objs = generateChunks(ranges);
	int num = chunk_objs.size();
	vector<vector<vector<hsize_t>>> mappings(num);
	vector<hsize_t> source_offsets;
	hsize_t source_row_size;
	if (mem_space_id == H5S_ALL) {
		source_offsets = calSerialOffsets(ranges, shape);
		source_row_size = ranges[ndims - 1][1] - ranges[ndims - 1][0] + 1;
	}
	else {
		vector<vector<hsize_t>> source_ranges = selectionFromSpace(mem_space_id);
		hsize_t dims_source[ndims];
		H5Sget_simple_extent_dims(mem_space_id, dims_source, NULL);
		vector<hsize_t> source_shape(dims_source, dims_source + ndims);
		source_offsets = calSerialOffsets(source_ranges, source_shape);
		source_row_size = source_ranges[ndims - 1][1] - source_ranges[ndims - 1][0] + 1;
	}

	for (int i = 0; i < num; i++) {
		hsize_t dest_row_size = chunk_objs[i]->ranges[ndims - 1][1] - chunk_objs[i]->ranges[ndims - 1][0] + 1;
		mappings[i] = mapHyperslab(chunk_objs[i]->local_offsets, chunk_objs[i]->global_offsets, source_offsets, 
						dest_row_size, source_row_size, data_size);
	}

	finish = 0;
    vector<int> s3_chunks;
    for (int i = 0; i < num; i++) {
        s3_chunks.push_back(i);
    }
	int s3_idx = 0;
	int s3_req_num = s3_chunks.size();
	int threadNum = s3Connections;
	vector<thread> s3_threads;
	s3_threads.reserve(threadNum);
	while(s3_idx < s3_req_num) {
		for (int idx = s3_idx; idx < min(s3_req_num, s3_idx + threadNum); idx++) {
			int i = s3_chunks[idx];
			int length = chunk_objs[i]->size;
			char* upload_buf = new char[length];

			for (auto &m: mappings[i]){
				memcpy(upload_buf + m[0], (char*)buf + m[1], m[2]);
			}

			thread s3_th(Operators::S3PutBuf, s3_client, bucket_name, chunk_objs[i]->uri, upload_buf, length);
			s3_threads.push_back(std::move(s3_th));
		}
		s3_idx = min(s3_req_num, s3_idx + threadNum);
#ifdef LOG_ENABLE
		cout << "idx: " << s3_idx << endl;
#endif
	    for (auto &t : s3_threads)
	    	t.join();
	    s3_threads.clear();
	}
	return SUCCESS;
}

vector<S3VLChunkObj*> S3VLDatasetObj::generateChunks(vector<vector<hsize_t>> ranges) {
	assert(ranges.size() == ndims);
	vector<int> accessed_chunks;
	// get accessed chunks
	vector<vector<hsize_t>> chunk_ranges(ndims);
	for (int i = 0; i < ndims; i++)
		chunk_ranges[i] = {ranges[i][0] / chunk_shape[i], ranges[i][1] / chunk_shape[i]};
	vector<hsize_t> chunk_offsets = calSerialOffsets(chunk_ranges, num_per_dim);
	for (int i = 0; i <= chunk_ranges[ndims - 1][1] - chunk_ranges[ndims - 1][0]; i++)
		for (auto &n : chunk_offsets)
			accessed_chunks.push_back(i + n);
	
	// iterate accessed chunks and generate queries
	vector<S3VLChunkObj*> chunk_objs;
	chunk_objs.reserve(accessed_chunks.size());
	Logger::log("------ # of chunks ", accessed_chunks.size());
	for (auto &c : accessed_chunks) {
		vector<hsize_t> offsets = getChunkOffsets(c);
		vector<vector<hsize_t>> local_ranges(ndims);
		for (int i = 0; i < ndims; i++) {
			hsize_t left = max(offsets[i], ranges[i][0]) - offsets[i];
			hsize_t right = min(offsets[i] + chunk_shape[i] - 1, ranges[i][1]) - offsets[i];
			local_ranges[i] = {left, right};
		}
		string chunk_uri = uri + "/" + std::to_string(c);
		FileFormat format = formats[c];

		// get output serial offsets for each row
		vector<vector<hsize_t>> global_ranges(ndims);
		vector<hsize_t> result_shape(ndims);
		for (int i = 0; i < ndims; i++) {
			global_ranges[i] = {local_ranges[i][0] + offsets[i] - ranges[i][0], 
								local_ranges[i][1] + offsets[i] - ranges[i][0]};
			result_shape[i] = ranges[i][1] - ranges[i][0] + 1;
		}
		vector<hsize_t> result_serial_offsets = calSerialOffsets(global_ranges, result_shape);

		
		S3VLChunkObj *chunk = new S3VLChunkObj(chunk_uri, format, dtype, local_ranges, chunk_shape, n_bits[c], result_serial_offsets);
		chunk_objs.push_back(chunk);
	}
	return chunk_objs;
}

// read/write

void S3VLDatasetObj::upload() {
	Logger::log("------ Upload metadata " + uri);
	int length;
	char* buffer = toBuffer(&length);
	string meta_name = uri + "/meta";
	Result re;
	re.data = buffer;
	re.length = length;
    Operators::S3Put(s3_client, bucket_name, meta_name, re);
}

S3VLDatasetObj* S3VLDatasetObj::getDatasetObj(S3Client *client, string bucket_name, string uri) {
    Result re = Operators::S3Get(client, bucket_name, uri);
	if (re.length == 0)
		return NULL;
	return S3VLDatasetObj::getDatasetObj(client, bucket_name, re.data);
}

char* S3VLDatasetObj::toBuffer(int *length) {
	int size = 8 + name.size() + uri.size() + sizeof(hid_t) + 4 + 2 * ndims * sizeof(hsize_t) + 4 + 2 * chunk_num * 4;
	*length = size;
	char * buffer = new char[size];
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
	memcpy(buffer + c, n_bits.data(), 4 * chunk_num);
	c += 4 * chunk_num;
	memcpy(buffer + c, formats.data(), 4 * chunk_num);
	return buffer;
}

S3VLDatasetObj* S3VLDatasetObj::getDatasetObj(S3Client *client, string bucket_name, char* buffer) {
	string name, uri;
	hid_t dtype;
	int c = 0;
	int name_length = *(int*)buffer;
	c += 4;
	name.assign(buffer + c, name_length);	// name
	c += name_length;
	int uri_length = *(int*)(buffer + c);
	c += 4;
	uri.assign(buffer + c, uri_length);    // uri
	c += uri_length;
	dtype = (*(hid_t*)(buffer + c));	// data type
	c += sizeof(hid_t);
	int ndims = *(int*)(buffer + c);	//ndims
	c += 4;
	vector<hsize_t> shape(ndims);	// shape
	memcpy(shape.data(), (hsize_t*)(buffer + c), sizeof(hsize_t) * ndims);
	c += sizeof(hsize_t) * ndims;
	vector<hsize_t> chunk_shape(ndims);	// chunk_shape
	memcpy(chunk_shape.data(), (hsize_t*)(buffer + c), sizeof(hsize_t) * ndims);
	c += sizeof(hsize_t) * ndims;
	int chunk_num = *(int*)(buffer + c);	// chunk num
	c += 4;
	vector<int> n_bits(chunk_num);
	memcpy(n_bits.data(), (int*)(buffer + c), chunk_num * 4);	// nbits
	c += 4 * chunk_num;
	vector<FileFormat> formats(chunk_num);
	for (int i = 0; i < chunk_num; i++) {
		int f = *(int*)(buffer + c);
		c += 4;
        formats[i] = binary;
	}
	return  new S3VLDatasetObj(name, uri, dtype, ndims, shape, chunk_shape, formats, n_bits, chunk_num, client, bucket_name);
}

string S3VLDatasetObj::to_string() {
	stringstream ss;
	ss << name << " " << uri << endl;
	ss << dtype << " " << ndims << endl;
	ss << chunk_num << " " << element_per_chunk << endl;;
	for (int i = 0; i < ndims; i++) {
		ss << shape[i] << " ";
	}
	ss << endl;
	for (int i = 0; i < ndims; i++) {
		ss << chunk_shape[i] << " ";
	}
	ss << endl;
	for (int i = 0; i < ndims; i++) {
		ss << num_per_dim[i] << " ";
	}
	ss << endl;
	for (int i = 0; i < ndims; i++) {
		ss << reduc_per_dim[i] << " ";
	}
	ss << endl;
	for (auto &f: formats)
		ss << f;
	ss << endl;
	for (auto &n: n_bits)
		ss << n;
	ss << endl;
	return ss.str();
}
