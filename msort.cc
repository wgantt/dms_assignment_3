#include <cstdlib>
#include <cstdio>

#include "library.h"
#include "json/json.h"

using namespace std;

int main(int argc, char* argv[]) {
  if (argc < 7) {
    cout << "ERROR: invalid input parameters!" << endl;
    cout << "Please enter <schema_file> <input_file> <output_file> <mem_capacity> <k> <sorting_attributes>" << endl;
    exit(1);
  }

  // Read in command line arguments
  string schema_file(argv[1]);
  char *input_file = argv[2];
  char *output_file = argv[3];
  long mem_capacity = atol(argv[4]);
  int k = atoi(argv[5]);
  string sort_attribute(argv[6]); // assuming a single sort attribute for now

  // Determine allowed buffer size, given mem_capacity and k
  int buf_size = mem_capacity / k;

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
  schema.n_sort_attrs = 1; // TODO: change if we end up supporting multiple sort attributes
  schema.sort_attrs = (int*) malloc(sizeof(int) * schema.n_sort_attrs);

  // Variables for loading an attribute
  string attr_name;
  string attr_type;
  int attr_len;
  
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

    // If this is a sorting attribute, add it to the list
    // of sort attributes
    if (sort_attribute.compare(attr_name) == 0) {
      schema.sort_attrs[0] = i;
    }
  }

  // Figure out how to determine run_length dynamically
  int run_length = 10;
  int num_runs = 10;

  // First phase: Make the runs
  mk_runs(input_file, output_file, run_length, &schema);

  // Create run iterators for in-memory sort
  RunIterator* iters[10];
  for (int i = 0; i < 10; i++) {
    iters[i] = new RunIterator("new.out", 270 * i, run_length, 260, &schema);
  }

  // Second phase: Do in-memory sort
  Attribute sort_attr = schema.attrs[schema.sort_attrs[0]];
  bool is_numeric = strcmp(sort_attr.type, INTEGER) || strcmp(sort_attr.type, FLOAT);
  RecordCompare rc {sort_attr.offset, sort_attr.length, is_numeric};
  merge_runs(iters, num_runs, "new.out", 0, 270, rc);

  // Free the iterators
  for (int i = 0; i < 10; i++) {
    free(iters[i]);
  }
  
  return 0;
}
