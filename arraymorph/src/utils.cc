#include "utils.h"
#include <algorithm>
#include <sstream>
vector<hsize_t> reduceAdd(vector<hsize_t> a, vector<hsize_t> b) {
	vector<hsize_t> re;
	re.reserve(a.size() * b.size());
	for (auto &i : a)
		for (auto &j: b)
			re.push_back(i + j);
	return re;
}
// calculate each row's start position in serial order
vector<hsize_t> calSerialOffsets(vector<vector<hsize_t>> ranges, vector<hsize_t> shape) {
	assert(ranges.size() == shape.size());
	int dims = ranges.size();
	vector<hsize_t> offsets_per_dim(dims);
	vector<vector<hsize_t>> offsets(dims - 1);

	offsets_per_dim[dims - 1] = 1;
	for (int i = dims - 2; i >= 0; i--)
		offsets_per_dim[i] = offsets_per_dim[i + 1] * shape[i + 1];
	
	for (int i = 0; i < dims - 1; i++)
		for (int j = ranges[i][0]; j <= ranges[i][1]; j++)
			offsets[i].push_back(j * offsets_per_dim[i]);
	vector<hsize_t> re = {0};
	for (int i = 0; i < dims - 1; i++)
		re = reduceAdd(re, offsets[i]);
	for (auto &o : re)
		o += ranges[dims - 1][0];

	return re;
}

// mapping offsets between two hyperslab

vector<vector<hsize_t>> mapHyperslab(vector<hsize_t> &local, vector<hsize_t> &global, vector<hsize_t> &out, 
	hsize_t &input_rsize, hsize_t &out_rsize, hsize_t &data_size) {
	vector<vector<hsize_t>> mapping;

	for (int i = 0; i < local.size(); i++) {
		hsize_t idx = global[i] / out_rsize;
		hsize_t start_offset = out[idx] + global[i] % out_rsize;
		hsize_t remaining = out[idx] + out_rsize - start_offset;
		if (remaining >= input_rsize) {
			mapping.push_back({local[i], start_offset, input_rsize});
		}
		else {
			mapping.push_back({local[i], start_offset, remaining});
			hsize_t cur = remaining;
			int n = (input_rsize - remaining) / out_rsize;
			for (int j = 1; j <= n; j++) {
				mapping.push_back({local[i] + cur, out[idx + j], out_rsize});
				cur += out_rsize;
			}
			if ((input_rsize - remaining) % out_rsize > 0) {
				mapping.push_back({local[i] + cur, out[idx + n + 1], (input_rsize - remaining) % out_rsize});
			}

		}
	}
	for (auto &m : mapping)
		for (auto &o : m)
			o *= data_size;
	return mapping;

}

string createQuery(hsize_t data_size, int ndims, vector<hsize_t> shape, vector<vector<hsize_t>> ranges) {
	stringstream ss;
	ss << "-" << data_size << "-" << ndims << "-";
	for (auto &s: shape)
		ss << s << "-";
	for (auto &r: ranges) 
		ss << r[0] << "-" << r[1] << "-";
	string re = ss.str();
    return re.substr(0, re.size() - 1);
}
