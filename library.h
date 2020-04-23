#include <cstdio>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <queue>
#include <string>
#include <functional>
#include <algorithm>
#include <iterator>
#include <cassert>
#include <cmath>
#include <cstring>

using namespace std;

// Named constants for numerical attribute types
static const char* INTEGER = "integer";
static const char* FLOAT = "float";

/**
 * The attribute schema
 */
typedef struct {
  char *name;
  char *type;
  int length;
  int offset;
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
 * Stores a record, along with the index of the input buffer
 * that it came from
 */
typedef struct {
  char* data;
  int buf_idx;
} BufRecord;

/**
 * Function object for comparing two records
 */
typedef struct {

  // The byte offset of the sorting attribute within the string
  int offset;

  // The length of the sorting attribute
  int attr_len;

  // Whether the sorting attribute is numeric
  bool is_numeric;

  // The comparison operator. Handles both string and numerical attributes.
  bool operator() (char* r1, char* r2) {
    string s1(r1);
    string s1_sort_attr = s1.substr(offset, attr_len);
    string s2(r2);
    string s2_sort_attr = s2.substr(offset, attr_len);
    if (is_numeric) {
      return atof(s1_sort_attr.c_str()) < atof(s2_sort_attr.c_str());
    } else {
      return s1_sort_attr < s2_sort_attr;
    }
  }
} RecordCompare;

/**
 * Comparison function object for comparing buffered records during the merge
 */
typedef struct {

  // Based on a RecordCompare struct
  RecordCompare rc;

  bool operator() (BufRecord r1, BufRecord r2) { return !rc(r1.data, r2.data); }

} BufRecordCompare;

/**
 * Priority queue for performing k-way merge
 */
typedef priority_queue<BufRecord,vector<BufRecord>,BufRecordCompare> MergePriorityQueue;

/**
 * The iterator helps you scan through a run.
 * you can add additional members as your wish
 */
class RunIterator {

// TODO: make private when finished testing
public:

  // The name of the file containing the records for this run
  char *filename;

  // The position in the file of the first record in this run,
  // specified as a record index
  long start_pos;

  // The start position of the next section of the run
  // (necessary only if the entire run does not fit in the
  // buffer)
  long next_section_pos;

  // The number of records in the run
  long run_length;

  // The size of the buffer (in bytes) used to read the run
  long buf_size;

  // The number of records that can fit in the buffer
  long buf_record_capacity;

  // The buffer
  char *buf;

  // Current record index within the RUN
  long record_idx;

  // Current record index within the BUFFER
  long buf_record_idx;

  // The record schema
  Schema *schema;

  // The current record the iterator is pointing to
  char *cur_record;

  /**
   * Alternative constructor to initialize the iterator without
   * actually loading the run
   */
  RunIterator(long buf_size, Schema *schema);

  /**
   * destructor
   */
  ~RunIterator();

  /**
   * resets the run iterator without allocating new memory.
   * The buffer size and schema are inherited from the original
   * initialization.
   */
  void reset(char *filename, long start_pos, long run_length);

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
int mk_runs(char *in_filename, char *out_filename, long run_length, Schema *schema);

/**
 * Merge runs given by the `iterators`.
 * The number of `iterators` should be equal to the `num_runs`.
 * Write the merged runs to `out_fp` starting at position `start_pos`.
 * Cannot use more than `buf_size` of heap memory allocated to `buf`.
 */
void merge_runs(RunIterator* iterators[], int num_runs, char *out_filename,
                long start_pos, long buf_size, RecordCompare rc);



// CustomComparator::CustomComparator(Schema *schema) {
//   schema = schema;
// }
// }

/**
 * Creates LevelDB database using custom comparator.
 * If there exists a database named as the file system directory, raise an error.
 */
// void create_db(leveldb::DB *db, const char* directory, Schema *schema); 

/**
 * Scans through the records and insert them one by one into the leveldb index.
 */
// void insert_leveldb(char *in_filename, leveldb::DB *db, Schema *schema);
