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

  // k input buffers for merging + 1 output buffer
  int buf_size = mem_capacity / (k + 1);

  // The length of a run is measured in # of records and is initially
  // determined by the size of the buffer and the total length
  // of a record (+1 for null-terminating character)
  int run_length = buf_size / (schema.total_record_length + 1);

  // The run length for the previous pass
  int prev_run_length = run_length;

  // Helper files for reading and writing runs
  char* helper = (char*) "helper.txt";
  char* helper2 = (char*) "helper2.txt";

  // First phase: Make the runs
  int num_runs = mk_runs(input_file, helper, run_length, &schema);

  // The number of passes we have to do for the merge is log_k(num_runs)
  int num_passes = ceil(log(num_runs) / log(k));

  cout << "buf_size : " << buf_size << ", run_length : " << run_length << 
        ", num_runs : " << num_runs <<", num_passes : " << num_passes << endl;

  // Second phase: Do in-memory sort

  // The attribute to sort on
  Attribute sort_attr = schema.attrs[schema.sort_attrs[0]];

  // Indicates whether the sort attirubte is a numeric value
  bool is_numeric = (strcmp(sort_attr.type, INTEGER) == 0) || (strcmp(sort_attr.type, FLOAT) == 0);

  // Struct for comparing records
  RecordCompare rc {sort_attr.offset, sort_attr.length, is_numeric};

  // Array for the k input buffers
  RunIterator* iters[k];

  // The number of runs that have been sorted so far on the current pass
  int runs_sorted = 0;

  // The number of buffers required for the current merge operation
  // (This is k, except at the very end of the list of runs)
  int buffers_needed = k;

  // The starting position in the input file for the merge operation
  int merge_start_pos = 0;

  // TODO: comment this
  char* curr_pass_input = helper;
  char* curr_pass_output = helper2;

  // Repeat for the required number of passes
  for (int pass = 0; pass < num_passes; pass++) {

    // If this is the final pass, ensure we're writing to the output file
    if (pass == num_passes - 1) {
      curr_pass_output = output_file;
    }

    // Do one pass of the sort
    while (runs_sorted != num_runs) {

      // The number of runs remaining to be sorted
      int runs_remaining = num_runs - runs_sorted;

      // The number of buffers we actually need for the current merge iteration.
      // This will be < k when we reach the end of the input file.
      buffers_needed = runs_remaining < k ? runs_remaining : k;

      // Allocate the buffers needed to merge these runs
      for (int j = 0; j < buffers_needed; j++) {

        // The start position for the current run
        int start_pos = (run_length * (schema.total_record_length + 1)) * runs_sorted;

        // If this is the start position for the first of the runs, this will
        // also be the start position for the current merge operation.
        if (j == 0) {
          merge_start_pos = start_pos;
        }

        // Allocate the buffer for this run
        iters[j] = new RunIterator(curr_pass_input, start_pos, run_length, buf_size, &schema);

        // The number of runs that have been sorted so far. 
        runs_sorted++;
      }

      // Merge the runs
      merge_runs(iters, buffers_needed, curr_pass_output, merge_start_pos, buf_size, rc);

      // Free the buffers
      for (int j = 0; j < buffers_needed; j++) {
        free(iters[j]);
      }
    }

    // Runs are now at most k times their previous length
    run_length *= k;

    // Update the number of runs for the next iteration
    num_runs = ceil((double) num_runs / k);

    // Reset the number of sorted runs
    runs_sorted = 0;

    // Swap input and output files
    char* temp = curr_pass_input;
    curr_pass_input = curr_pass_output;
    curr_pass_output = temp;
  }
  return 0;
}
