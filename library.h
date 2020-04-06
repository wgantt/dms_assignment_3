#include <cstdio>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <functional>

using namespace std;

/**
 * TODO:
 *   - Figure out how, if at all, you intend to use data structures other
 *     than Schema.
 *   - Correctly set total_record_length for Schema
 */

/**
 * An attribute schema. You should probably modify
 * this to add your own fields.
 */
typedef struct {
  char *name;
  int length;
} Attribute;

/**
 * A record schema contains an array of attribute
 * schema `attrs`, as well as an array of sort-by 
 * attributes (represented as the indices of the 
 * `attrs` array).
 */
typedef struct {
  int nattrs;
  int* sort_attrs;
  int n_sort_attrs;
  int total_record_length = 0;
  Attribute* attrs;
} Schema;

/**
 * A record can defined as a struct with a pointer
 * to the schema and some data. 
 */
typedef struct {
  Schema* schema;
  char* data;
} Record;

/**
 * The iterator helps you scan through a run.
 * you can add additional members as your wish
 */
class RunIterator {

private:

  // The name of the file containing the records for this run
  char *filename;

  // The position in the file of the first record in this run,
  // specified as a record index
  long start_pos;

  // The number of records in the run
  long run_length;

  // The size of the buffer used to read the run
  long buf_size;

  // The buffer
  char *buf;

  // Current record index within the buffer
  long record_idx;

  // The record schema
  Schema *schema;

  // The current record the iterator is pointing to
  char *cur_record;

public:

  /**
   * Creates an interator using the `buf_size` to
   * scan through a run that starts at `start_pos`
   * with length `run_length`.
   */
  RunIterator(char *filename, long start_pos, long run_length, long buf_size,
              Schema *schema);

  ~RunIterator();

  /**
   * reads the next record
   */
  char* next();

  /**
   * return false if iterator reaches the end
   * of the run
   */
  bool has_next();
};

/**
 * Creates sorted runs of length `run_length` in
 * the `out_fp`.
 */
void mk_runs(char *in_filename, char *out_filename, long run_length, Schema *schema);

/**
 * Merge runs given by the `iterators`.
 * The number of `iterators` should be equal to the `num_runs`.
 * Write the merged runs to `out_fp` starting at position `start_pos`.
 * Cannot use more than `buf_size` of heap memory allocated to `buf`.
 */
void merge_runs(RunIterator* iterators[], int num_runs, char *out_filename,
                long start_pos, char *buf, long buf_size);