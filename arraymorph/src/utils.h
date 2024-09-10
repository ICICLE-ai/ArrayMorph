#ifndef UTILS
#define UTILS
#include <iostream>
#include <vector>
#include <assert.h>
#include "constants.h"
#include <hdf5.h>
using namespace std;
vector<hsize_t> reduceAdd(vector<hsize_t> a, vector<hsize_t> b);
// calculate each row's start position in serial order
vector<hsize_t> calSerialOffsets(vector<vector<hsize_t>> ranges, vector<hsize_t> shape);
// mapping offsets between two hyperslab
vector<vector<hsize_t>> mapHyperslab(vector<hsize_t> &local, vector<hsize_t> &global, vector<hsize_t> &out, 
	hsize_t &input_rsize, hsize_t &out_rsize, hsize_t &data_size);
// create Json query for Lambda function
string createQuery(hsize_t data_size, int ndims, vector<hsize_t> shape, vector<vector<hsize_t>> ranges);
#endif
