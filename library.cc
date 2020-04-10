#include "library.h"

using namespace std;

void mk_runs(char *in_filename, char *out_filename, long run_length, Schema *schema)
{
  	// Streams for reading in data and writing sorted runs
	ifstream in_file(in_filename);
	ofstream out_file(out_filename);

	// Error if unable to open streams
	if (!in_file.is_open()) {
    	cout << "could not open " << in_filename << endl;
    	exit(1);
  	} else if (!out_file.is_open()) {
  		cout << "could not open " << out_filename << endl;
    	exit(1);
  	}

	// The current record being read
	string record;

	// Stores the current attribute of the current record
	string attribute;

	// Vector for the current record
	vector<string> record_vect;

	// Vector to hold all records in the current runt
	vector<vector<string>> run_records;

	// The global index of the current record (across all runs)
	int record_idx = 0;

	// The attribute to sort on
	// TODO: support multi-attribute sorting
	int sort_attr_idx = schema->sort_attrs[0];
	Attribute sort_attr = schema->attrs[sort_attr_idx];
	bool is_numeric = strcmp(sort_attr.type, INTEGER) || strcmp(sort_attr.type, FLOAT);

	// Lambda for comparing records
	auto comp = [sort_attr_idx, is_numeric] (vector<string> r1, vector<string> r2) {
		if (is_numeric) {
			return atof(r1[sort_attr_idx].c_str()) < atof(r2[sort_attr_idx].c_str());
		} else {
			return (bool) r1[sort_attr_idx].compare(r2[sort_attr_idx]);
		}
	};

	// Read in the header (we assume that the schema contains the same
	// information, so this can be ignored).
	getline(in_file, record);

	// Read in records
	while (getline(in_file, record)) {

		// If we've completed a run, sort it and write it to the file
		if ((record_idx != 0) && (record_idx % run_length == 0)) {

			// sort the records in this run
			sort(run_records.begin(), run_records.end(), comp);

			// write the records to the output file
			for (auto it = run_records.begin(); it != run_records.end(); it++) {
				vector<string> cur_record = *it;
				for (auto it2 = cur_record.begin(); it2 != cur_record.end(); it2++) {
					out_file << *it2;
				}
				out_file << endl;
			}

			// clear the run vector
			run_records.clear();
		}

		// Read in the attributes of the record
		istringstream recordStream(record);
		while (getline(recordStream, attribute, ',')) {
			record_vect.push_back(attribute);
		}

		// Add the current record to the vector of all records for this run
		run_records.push_back(record_vect);
		record_vect.clear();

		// Increment the record number
		record_idx++;
	}

	// Sort and write any remaining records
	sort(run_records.begin(), run_records.end(), comp);
	for (auto it = run_records.begin(); it != run_records.end(); it++) {
		vector<string> cur_record = *it;
		for (auto it2 = cur_record.begin(); it2 != cur_record.end(); it2++) {
			out_file << *it2;
		}
		out_file << endl;
	}

	// Close streams
	in_file.close();
	out_file.close();
}

void merge_runs(RunIterator* iterators[], int num_runs, char *out_filename,
                long start_pos, long buf_size, RecordCompare rc)
{
	// Allocate the output buffer (we assume it's unallocated to begin with)
	char* buf = new char[buf_size];

	// Open the output file for writing
	ofstream out(out_filename);
	if (!out.is_open()) {
		cerr << "Unable to open output file for merging runs" << endl;
		exit(1);
	}

	// Initialize priority queue for k-way merge
	BufRecordCompare brc {rc};
	MergePriorityQueue pq(brc);

	// Initialize priority queue with the first record in each buffer
	BufRecord cur_record;
	for (int i = 0; i < num_runs; i++) {
		cur_record.data = iterators[i]->next();
		cur_record.buf_idx = i;
		pq.push(cur_record);
		cout << cur_record.data << endl;
	}

	cout << endl << endl;

	while (!pq.empty()) {
		cout << pq.top().data << endl;
		pq.pop();
	}

	// Continue merging records until there are no more
	BufRecord next_record;
	while (!pq.empty()) {

		// Pop the next record from the top of the priority queue
		BufRecord cur_record = pq.top();
		pq.pop();

		// Check the buffer that this record came from to see whether
		// it contains any more records. If it does, increment the
		// iterator for that buffer and add the next record to the queue
		if (iterators[cur_record.buf_idx]->has_next()) {
			next_record.data = iterators[cur_record.buf_idx]->next();
			next_record.buf_idx = cur_record.buf_idx;
			pq.push(next_record);
		}

		// Copy it into the output buffer
		strcat(buf, cur_record.data);

		// If the output buffer is full (or nearly so), flush it to disk and clear the buffer
		if (strlen(buf) + strlen(cur_record.data) > buf_size) {
			memset(buf,0,buf_size);
		}
	}

	cout << "pq is now empty" << endl;

	// Close the output file and ree the output buffer
	out.close();
	free(buf);
}

RunIterator::RunIterator(char *filename, long start_pos, long run_length, long buf_size,
              Schema *schema) {

	// Initialize member variables (see library.h for descriptions of each)
	this->filename = filename;
	this->start_pos = start_pos;
	this->run_length = run_length;
	this->buf_size = buf_size;
	this->record_idx = 0;
	this->schema = schema;
	this->buf = new char[buf_size];
	this->cur_record = new char[this->schema->total_record_length + 1];

	// Open the file for reading
	ifstream in_file(filename);
	if (!in_file.is_open()) {
		cout << "could not open " << filename << endl;
		exit(1);
	}

	// Verify that the buffer is of adequate size to store all records in
	// the run
	if (buf_size < run_length * (schema->total_record_length + 1)) {
		cerr << "Buffer size is too small to store all records in run" << endl;
		exit(1);
	}

	// Set the start position within the file. We assume that
	// the start position lands us at the beginning of a record.
	in_file.seekg(start_pos);

	// Read records one by one into the buffer
	string record;
	int record_idx = 0;
	while (getline(in_file, record) && record_idx < run_length) {
		strcat(buf, record.c_str());
		record_idx++;
	}

	// Close the stream for reading
	in_file.close();
}

RunIterator::~RunIterator() {
	free(this->buf);
	free(this->cur_record);
}

char* RunIterator::next() {
	// Copy record from buffer
	int record_len = this->schema->total_record_length + 1;
	strncpy(this->cur_record, &this->buf[record_idx * record_len], record_len);

	// Shouldn't have to explicitly null-terminate,
	// but didn't take the time to figure a way around it
	this->cur_record[record_len - 1] = '\0';

	// Increment iterator index
	this->record_idx++;

	// Return record
	return this->cur_record;
}

bool RunIterator::has_next() {
	return this->record_idx < this->run_length;
}