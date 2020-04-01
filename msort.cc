#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>

#include "library.h"
#include "json/json.h"

using namespace std;

int main(int argc, const char* argv[]) {
  if (argc < 7) {
    cout << "ERROR: invalid input parameters!" << endl;
    cout << "Please enter <schema_file> <input_file> <output_file> <mem_capacity> <k> <sorting_attributes>" << endl;
    exit(1);
  }

  // Read in command line arguments
  string schema_file(argv[1]);
  string input_file(argv[2]);
  string output_file(argv[3]);
  long mem_capacity = atol(argv[4]);
  int k = atoi(argv[5]);
  string sort_attribute(argv[6]); // assuming a single sort attribute for now

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
  int attr_len;
  
  // Load and print out the schema
  for (int i = 0; i < json_schema.size(); ++i) {
    attr_name = json_schema[i].get("name", "UTF-8" ).asString();
    attr_len = json_schema[i].get("length", "UTF-8").asInt();
    cout << "{name : " << attr_name << ", length : " << attr_len << "}" << endl;

    // Create an Attribute struct for the current attribute
    Attribute attribute;
    attribute.name = (char*) malloc(sizeof(attr_name.c_str()));
    strcpy(attribute.name, attr_name.c_str());
    attribute.length = attr_len;

    // Add attribute to the schema
    schema.attrs[i] = attribute;

    // If this is a sorting attribute, add it to the list
    // of sort attributes
    if (sort_attribute.compare(attr_name) == 0) {
      schema.sort_attrs[0] = i;
    }
  }

  // Do the sort
  // Your implementation
  
  return 0;
}
