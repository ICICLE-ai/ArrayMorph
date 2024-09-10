#ifndef S3VL_CHUNK_OBJ
#define	S3VL_CHUNK_OBJ
#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <set>
#include "constants.h"
#include "operators.h"
#include "logger.h"
#include <hdf5.h>
using namespace std;

typedef struct CPlan
{
	int chunk_id;
	QPlan qp;
	int row_id=0;
} CPlan;

class S3VLChunkObj
{
public:
	S3VLChunkObj(string uri, FileFormat format, hid_t dtype, vector<vector<hsize_t>> ranges,
			vector<hsize_t> shape, int n_bits, vector<hsize_t> return_offsets);
	~S3VLChunkObj(){};

	string to_string();
	bool checkFullWrite();
	vector<hsize_t> shape;
	FileFormat format;
	vector<vector<hsize_t>> ranges;
	vector<hsize_t> reduc_per_dim;
	int ndims;
	string uri;
	hid_t dtype;
	int n_bits;
	int data_size;
	hsize_t size;
	int id;
	hsize_t required_size;
	vector<hsize_t> global_offsets;
	vector<hsize_t> local_offsets;
};

#endif
