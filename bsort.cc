#include <cstdlib>
#include "library.h"
#include "json/json.h"
#include "leveldb/db.h" 
#include "leveldb/comparator.h"
#include "leveldb/slice.h"

using namespace std;

/**
 * A custom comparator subclassing leveldb Comparator class.
 * Compares records by the sorting attributes.
 */
class CustomComparator : public leveldb::Comparator {
  public:
    Schema *schema;

    const char* Name() const { return "CustomComparator"; }

	CustomComparator(Schema *schema_passed): leveldb::Comparator() {
	  schema = schema_passed;
	}

    int Compare(const leveldb::Slice& key1, const leveldb::Slice& key2) const {

      int offset; // offset of an attribute
      int attr_len; // length of an attribute
      int attr_idx; // position of an attribute

      Attribute sort_attr;
	  leveldb::Slice s1_sort_attr;
      leveldb::Slice s2_sort_attr;
	  
	  // Comparing multiple attribute values in slice
      for(int i = 0; i < schema->n_sort_attrs; i++) {
        attr_idx = schema->sort_attrs[i];
        sort_attr = schema->attrs[attr_idx];
        attr_len = sort_attr.length;
		offset = sort_attr.offset;

		// Drop the first n-bytes from the key
		// Create a slice that refers to the contents of the sorting attribute
		s1_sort_attr = key1;
		s1_sort_attr.remove_prefix(offset + attr_idx);
		s1_sort_attr = leveldb::Slice(s1_sort_attr.data(), attr_len);

		s2_sort_attr = key2;
		s2_sort_attr.remove_prefix(offset + attr_idx);
		s2_sort_attr = leveldb::Slice(s2_sort_attr.data(), attr_len);

		if (s1_sort_attr.compare(s2_sort_attr) < 0) {
			return -1;
		}
		if (s1_sort_attr.compare(s2_sort_attr) > 0) {
			return +1;
		}
	  }

      return 0;
    }

    void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    void FindShortSuccessor(std::string*) const {}

};

int main(int argc, char* argv[]) {

	if (argc < 5) {
		cout << "ERROR: invalid input parameters!" << endl;
		cout << "Please enter <schema_file> <input_file> <out_index> <sorting_attributes>" << endl;
		exit(1);
	}

	// Read in command line arguments
	string schema_file(argv[1]);
	char *input_file = argv[2];
	char *output_file = argv[3];
	std::vector<std::string> sort_attributes; // sorting attribute storage

	// Iterate through the sorting attributes and
	// put each into the sorting attribute storage
    for (int i = 4; i < argc; ++i) {
        std::string attr = argv[i];
		sort_attributes.push_back(attr);
	}

	// Parse the schema JSON file
	Json::Value json_schema;
	Json::Reader json_reader;

	// Support for std::string argument is added in C++11
	// so you don't have to use .c_str() if you are on that.
	ifstream schema_file_istream(schema_file.c_str(), ifstream::binary);
	bool successful = json_reader.parse(schema_file_istream, json_schema, false);
	if (!successful) {
		cout << "ERROR: " << json_reader.getFormatedErrorMessages() << endl;
		exit(1);
	}

	// Struct to store the schema
	Schema schema;
	schema.nattrs = json_schema.size();
	schema.attrs = (Attribute*) malloc(sizeof(Attribute) * schema.nattrs);
	schema.n_sort_attrs = argc - 4;
	schema.sort_attrs = (int*) malloc(sizeof(int) * schema.n_sort_attrs);

	// Variables for loading an attribute
	string attr_name;
	string attr_type;
	int attr_len;
	int sort_attr_count = 0;

	// Load and print out the schema
	for (int i = 0; i < json_schema.size(); ++i) {
		attr_name = json_schema[i].get("name", "UTF-8" ).asString();
		attr_len = json_schema[i].get("length", "UTF-8").asInt();
		attr_type = json_schema[i].get("type", "UTF-8").asString();
		cout << "{name : " << attr_name << ", length : " << attr_len << "}" << endl;

		// Create an Attribute struct for the current attribute
		Attribute attribute;
		attribute.name = (char*) malloc(sizeof(attr_name.c_str()));
		strcpy(attribute.name, attr_name.c_str());
		attribute.type = (char*) malloc(sizeof(attr_type.c_str()));
		strcpy(attribute.type, attr_type.c_str());
		attribute.length = attr_len;
		attribute.offset = schema.total_record_length;

		// Add attribute to the schema and increment record length
		schema.attrs[i] = attribute;
		schema.total_record_length += attr_len;

		// Position of the attribute in the sorting attribute storage
		ptrdiff_t priority = distance(sort_attributes.begin(), 
							std::find(sort_attributes.begin(), sort_attributes.end(), attr_name));
		
		// If this is a sorting attribute
		if (priority < sort_attributes.size()) {
			// Add its sort attribute priority to the list of sort attributes
			schema.sort_attrs[priority] = i;
			sort_attr_count++;
		}
	}

	// Raise an error if a sorting attribute does not exist in schema
	if (sort_attr_count != schema.n_sort_attrs){
		cout << "ERROR: invalid sorting attribute name"  << endl;
		exit(1);
	}

	// Creating a leveldb database using custom comparator
	leveldb::DB *db;
	CustomComparator cmp(&schema);
	leveldb::Options options;
	options.create_if_missing = true;
	options.comparator = &cmp;

	// If there exists a database named as the file system directory, raise an error
	options.error_if_exists = true;

	// creates a database connection to the directory argument
	leveldb::Status status = leveldb::DB::Open(options, "../leveldb_dir", &db);
	if (!status.ok()) cerr << status.ToString() << endl;
	assert(status.ok());

	// Stream for reading in data
	ifstream in_file(input_file);
	ofstream out_file(output_file);

  	// Error if unable to open stream
	if (!in_file.is_open()) {
		cout << "could not open " << input_file << endl;
		exit(1);
	}
	else if (!out_file.is_open()) {
  		cout << "could not open " << output_file << " to write results" << endl;
    	exit(1);
  	}
  
  	// The current record being read
	std::string record;

	// Read in the header
	getline(in_file, record);

	// Read in records
	while (getline(in_file, record)) {
		// Read in the attributes of the record
		istringstream recordStream(record);

		leveldb::Slice key = record;
		leveldb::Slice value = record;

		// Insert all records into leveldb
		db->Put(leveldb::WriteOptions(), key, value);
	}

	// Close stream
	in_file.close();

	// Iterate through all key, value pairs in the database
	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		leveldb::Slice value = it->value();
		std::string val_str = value.ToString();

		// write the record to the output file
		out_file << val_str << endl;
	}
	// Check for any errors found during the scan
	assert(it->status().ok());

	// Close stream
	out_file.close();
	delete it;

	// Closing the database
	delete db;

	return 0;
}