#include "library.h"

using namespace std;

int mk_runs(char *in_filename, char *out_filename, long run_length, Schema *schema)
{
  	// Streams for reading in data and writing sorted runs
	ifstream in_file(in_filename);
	ofstream out_file(out_filename);

	// Error if unable to open streams
	if (!in_file.is_open()) {
    	cout << "could not open " << in_filename << " to create runs" << endl;
    	exit(1);
  	} else if (!out_file.is_open()) {
  		cout << "could not open " << out_filename << " to create runs" << endl;
    	exit(1);
  	}

	// The current record being read
	string record;

	// Stores the current attribute of the current record
	string attribute;

	// Vector for the current record
	vector<string> record_vect;

	// Vector to hold all records in the current run
	vector<vector<string>> run_records;

	// The global index of the current record (across all runs)
	int record_idx = 0;

	// The number of runs created
	int num_runs = 0;

	// The attribute to sort on
	// TODO: support multi-attribute sorting
	int sort_attr_idx = schema->sort_attrs[0];
	Attribute sort_attr = schema->attrs[sort_attr_idx];
	bool is_numeric = (strcmp(sort_attr.type, INTEGER) == 0) || (strcmp(sort_attr.type, FLOAT) == 0);

	// Lambda for comparing records
	auto comp = [sort_attr_idx, is_numeric] (vector<string> r1, vector<string> r2) {
		if (is_numeric) {
			return atof(r1[sort_attr_idx].c_str()) < atof(r2[sort_attr_idx].c_str());
		} else {
			return r1[sort_attr_idx] < r2[sort_attr_idx];
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

			// clear the run vector, increment the number of runs
			run_records.clear();
			num_runs++;
		}

		// Read in the attributes of the record (strip trailing whitespace)
		record = record.substr(0,record.size() - 1);
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

	if (!run_records.empty()) {
		num_runs++;
	}

	// Close streams
	in_file.close();
	out_file.close();

	return num_runs;
}

void merge_runs(RunIterator* iterators[], int num_runs, char *out_filename,
                long start_pos, long buf_size, char* buf, RecordCompare rc)
{
	// Open the file for writing
	ofstream out;
	if (start_pos == 0) {
		out = ofstream(out_filename);
	} else {
		out = ofstream(out_filename, ios::app);
	}

	// Open the output file for writing (appending)
	if (!out.is_open()) {
		cerr << "Unable to open output file for merging runs" << endl;
		exit(1);
	}

	// If there's exactly one run to be merged, just write it directly
	// to the output file, since it's already sorted
	if (num_runs == 1) {
		while (iterators[0]->has_next()) {
			out << iterators[0]->next() << endl;
		}
		out.close();
		return;
	}

	// The number of records that can be stored in the buffer
	long buf_record_capacity = iterators[0]->buf_record_capacity;

	// Zero out the output buffer
	memset(buf,0,buf_size);

	// Initialize priority queue for k-way merge
	BufRecordCompare brc {rc};
	MergePriorityQueue pq(brc);

	// Initialize priority queue with the first record in each buffer
	BufRecord cur_record;
	for (int i = 0; i < num_runs; i++) {
		cur_record.data = iterators[i]->next();
		cur_record.buf_idx = i;
		pq.push(cur_record);
	}

	// Stores the next record to be added to the priority queue
	BufRecord next_record;

	// The number of records currently stored in the buffer
	int records_in_buf = 0;

	// Continue merging records until there are no more
	while (!pq.empty()) {

		// Pop the next record from the top of the priority queue
		BufRecord cur_record = pq.top();
		pq.pop();

		// Copy it into the output buffer
		strcat(buf, cur_record.data);
		records_in_buf++;

		// If the output buffer is full (or nearly so), flush it to disk and clear the buffer
		if (records_in_buf == buf_record_capacity) {
			string s = string(buf);
			int record_len = strlen(cur_record.data);
			for (int i = 0; i < records_in_buf; i++) {
				out << s.substr(i * record_len, record_len) << endl;
			}
			memset(buf,0,buf_size);
			records_in_buf = 0;
		}

		// Check the buffer that this record came from to see whether
		// it contains any more records. If it does, increment the
		// iterator for that buffer and add the next record to the queue
		if (iterators[cur_record.buf_idx]->has_next()) {
			next_record.data = iterators[cur_record.buf_idx]->next();
			next_record.buf_idx = cur_record.buf_idx;
			pq.push(next_record);
		}
	}

	// Flush any records remaining in the buffer
	if (records_in_buf > 0) {
		string s = string(buf);
		int record_len = strlen(cur_record.data);
		for (int i = 0; i < records_in_buf; i++) {
			out << s.substr(i * record_len, record_len) << endl;
		}
		memset(buf,0,buf_size);
		records_in_buf = 0;
	}

	// Close the output file and ree the output buffer
	out.close();
}

RunIterator::RunIterator(long buf_size, Schema *schema) {
	this->buf_size = buf_size;
	this->buf = new char[buf_size];
	this->schema = schema;
	this->buf_record_capacity = this->buf_size / (this->schema->total_record_length + 1);
	this->cur_record = new char[this->schema->total_record_length + 1];
}

void RunIterator::reset(char *filename, long start_pos, long run_length) {
	this->filename = filename;
	this->start_pos = start_pos;
	this->run_length = run_length;
	this->next_section_pos = start_pos + ((schema->total_record_length + 1) * this->buf_record_capacity);
	this->record_idx = 0;
	this->buf_record_idx = 0;

	// clear the buffer and current record
	memset(this->buf, 0, this->buf_size);
	memset(this->cur_record, 0, this->schema->total_record_length + 1);

	// Open the file for reading
	ifstream in_file(filename);
	if (!in_file.is_open()) {
		cout << "could not open " << filename << " for creation of run iterator" << endl;
		exit(1);
	}

	// Set the start position within the file. We assume that
	// the start position lands us at the beginning of a record.
	in_file.seekg(start_pos);

	// Read records one by one into the buffer
	string record;
	int i = 0;
	while (getline(in_file, record) && i < this->buf_record_capacity) {
		strncat(buf, record.c_str(), record.size());
		i++;
	}

	// If i is ever less than the buf_record_capacity, it means the current
	// run length is less than we previously thought, so we update it
	if (i < this->buf_record_capacity) {
		this->run_length = i;
	} else {
		this->run_length = run_length;
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
	int record_len = this->schema->total_record_length;
	strncpy(this->cur_record, &this->buf[buf_record_idx * record_len], record_len);

	// Shouldn't have to explicitly null-terminate,
	// but didn't take the time to figure a way around it
	this->cur_record[record_len] = '\0';

	// Increment iterator index
	this->record_idx++;
	this->buf_record_idx++;

	// Return record
	return this->cur_record;
}

bool RunIterator::has_next() {
	// If we've reached the end of the buffer, we need to attempt to
	// load in the next records in the run from disk
	if (this->buf_record_idx == this->buf_record_capacity && 
		this->record_idx < this->run_length) {

		// Open the file for reading
		ifstream in_file(filename);
		if (!in_file.is_open()) {
			cout << "could not open " << filename << "for iterating over runs" << endl;
			exit(1);
		}

		// Determine the start position of the next section of the run
		in_file.seekg(next_section_pos);

		// Zero out the buffer for good measure
		memset(buf,0,buf_size);

		// Read records one by one into the buffer
		string record;
		int i = 0;
		while (getline(in_file, record) && i < this->buf_record_capacity) {
			strncat(buf, record.c_str(), record.size());
			i++;
		}

		// If i is ever less than the buf_record_capacity, it means the current
		// run length is less than we previously thought, so we update it
		if (i < this->buf_record_capacity) {
			this->run_length = this->record_idx + i;
		}

		// Close the stream
		in_file.close();

		// Update next_section_pos and reset buf_record_idx
		this->next_section_pos += ((schema->total_record_length + 1) * this->buf_record_capacity);
		this->buf_record_idx = 0;
	}
	return this->record_idx < this->run_length;
}