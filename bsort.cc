#include <cstdio>
#include <cstdlib>
#include "library.h"
#include "json/json.h"

using namespace std;

int main(int argc, char* argv[]) {

	if (argc < 4) {
		cout << "ERROR: invalid input parameters!" << endl;
		cout << "Please enter <schema_file> <input_file> <out_index> <sorting_attributes>" << endl;
		exit(1);
	}

	// Read in command line arguments
	string schema_file(argv[1]);
	char *input_file = argv[2];
	char *out_index = argv[3]; //output file
	std::vector<std::string> sort_attributes; // sorting attribute storage

	// iterate through the sorting attributes and
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
	schema.n_sort_attrs = argc - 3;
	schema.attr_prio = (int*) malloc(sizeof(int) * schema.nattrs);
	schema.prio_attr = (int*) malloc(sizeof(int) * schema.n_sort_attrs);
	schema.sort_attrs = (int*) malloc(sizeof(int) * schema.n_sort_attrs);

	// Variables for loading an attribute
	string attr_name;
	string attr_type;
	int attr_len;
	int sort_attr_idx;

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
			// Change its sort attribute priority to its position in the sorting attirbute sotrage.
			// Attributes with a non-negative and smaller value in the list have higher priority in sorting.
			schema.attr_prio[i] = priority;
			
			// add it to the list of sort attributes
			schema.prio_attr[priority] = i;

			// Add the sorting priority of the attirbute 
			// to the sorting attibute positioned among the N attributes
			schema.sort_attrs[sort_attr_idx] = priority;

			printf("sorting priority = %td\n", priority);
			sort_attr_idx++;
		} else {
			// For non-sorting attributes, the value is -1
			schema.attr_prio[i] = -1;
		}
	}

	// TODO: Check whether all sorting attributes exist in schema


	// Creating a leveldb database using custom comparator
	leveldb::DB *db;
	create_db(db, "./leveldb_dir", &schema);
	// TODO: check if leveldb_dir directory has to exist

	// Insert all records into leveldb
	insert_leveldb(input_file, db, &schema);

	// Iterate through all key, value pairs in the database
	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		leveldb::Slice key = it->key();
		leveldb::Slice value = it->value();
		std::string key_str = key.ToString();
		std::string val_str = value.ToString();
		cout << key_str << ": "  << val_str << endl;
		// TODO: write to out_index
	}
	// Check for any errors found during the scan
	assert(it->status().ok());
	delete it;

	// Closing the database
	delete db;

	return 0;
}