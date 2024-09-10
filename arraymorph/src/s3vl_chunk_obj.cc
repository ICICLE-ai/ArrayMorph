#include "s3vl_chunk_obj.h"
#include "utils.h"
#include <sstream>
#include <algorithm>
#include <assert.h>

S3VLChunkObj::S3VLChunkObj(string name, FileFormat format, hid_t dtype, vector<vector<hsize_t>> ranges,
			vector<hsize_t> shape, int n_bits, vector<hsize_t> return_offsets) {
	this->uri = name;
	this->format = format;
	this->dtype = dtype;
	this->data_size = H5Tget_size(dtype);
	this->ranges = ranges;
	this->shape = shape;
	this->ndims = shape.size();
	this->n_bits = n_bits;
	this->global_offsets = return_offsets;
	assert(this->ndims = ranges.size());

	vector<hsize_t>reduc_per_dim(this->ndims);
	reduc_per_dim[this->ndims - 1] = 1;
	for (int i = this->ndims - 2; i >= 0; i--)
		reduc_per_dim[i] = reduc_per_dim[i + 1] * shape[i + 1];
	this->reduc_per_dim = reduc_per_dim;
	this->local_offsets = calSerialOffsets(ranges, shape);
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

string S3VLChunkObj::to_string() {
	stringstream ss;
	ss << uri << " " << dtype << " " << format << " " << ndims << endl;
	for (auto &s : shape)
		ss << s << " ";
	ss << endl;
	for (int i = 0; i < ndims; i++) {
		ss << shape[i] << " [" << ranges[i][0] << ", " << ranges[i][1] << "]" << endl; 
	}
	ss << n_bits << endl;
	return ss.str();
}
