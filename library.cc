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
	int sort_attr = schema->sort_attrs[0];

	cout << sort_attr << endl;

	// Function for comparing records
	auto comp = [sort_attr] (vector<string> r1, vector<string> r2) { return r1[sort_attr] < r2[sort_attr]; };

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
					out_file << *it2 << ",";
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
                long start_pos, char *buf, long buf_size)
{
  // Your implementation
}

bool isComma(char c) { return c == ','; }

int str_compare(const void* p1, const void* p2) { return strcmp((char*) p1, (char*)p2); }