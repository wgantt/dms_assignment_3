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
#include "leveldb/db.h"
#include "leveldb/comparator.h"

using namespace std;

// ADD NAMED CONSTANTS FOR ATTRIBUTE TYPES
static const char* STRING = "string";
static const char* INTEGER = "integer";
static const char* FLOAT = "float";

/**
 * TODO:
 *   - Figure out how, if at all, you intend to use data structures other
 *     than Schema.
 */

/**
 * An attribute schema. You should probably modify
 * this to add your own fields.
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
  int* attr_prio;
  int* prio_attr;
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
      return s1_sort_attr.compare(s2_sort_attr);
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
int mk_runs(char *in_filename, char *out_filename, long run_length, Schema *schema);

/**
 * Merge runs given by the `iterators`.
 * The number of `iterators` should be equal to the `num_runs`.
 * Write the merged runs to `out_fp` starting at position `start_pos`.
 * Cannot use more than `buf_size` of heap memory allocated to `buf`.
 */
void merge_runs(RunIterator* iterators[], int num_runs, char *out_filename,
                long start_pos, long buf_size, RecordCompare rc);

/**
 * A custom comparator subclassing leveldb Comparator class.
 * Compares records by the sorting attributes.
 */
// TODO: move this class somewhere?
leveldb::Comparator::~Comparator() = default;
// namespace {
class CustomComparator : public leveldb::Comparator {
  public:

    Schema *schema;

    const char* Name() const { return "CustomComparator"; }
    //TODO: need to change for additional parameters?

    CustomComparator(Schema *schema);

    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
      // sorting values in slice converted back to a C++ string
      std::string key1 = a.ToString();
      std::string key2 = b.ToString();

      // Storage for sorting attribute values
      int sort_offsets[schema->n_sort_attrs];

      // offset of an attribute among sorting attributes
      int offset;
      int attr_len;
      int sort_attr_priority;
      int attr_idx;

      Attribute sort_attr;
      string s1_sort_attr;
      string s2_sort_attr;
      
      for(int i = 0; i < schema->n_sort_attrs; i++) {
        sort_attr_priority = schema->sort_attrs[i];

        // Add sorting attribue offset to its corresponding sorting priority/index
        sort_offsets[sort_attr_priority] = offset;

        attr_idx = schema->prio_attr[sort_attr_priority];
        attr_len = schema->attrs[attr_idx].length;
        offset += attr_len;
      }

      // Compares multiple attributes of two records
      // in the order of their priority
      for(int i = 0; i < schema->n_sort_attrs; i++) {
        attr_idx = schema->prio_attr[i];
        sort_attr = schema->attrs[attr_idx];
        attr_len = sort_attr.length;

        s1_sort_attr = key1.substr(offset, attr_len);
        s2_sort_attr = key2.substr(offset, attr_len);

        bool is_numeric = (strcmp(sort_attr.type, INTEGER) == 0) ||
                          (strcmp(sort_attr.type, FLOAT) == 0);

        if (is_numeric) {
          // Handles numerical attributes
          int val1 = atof(s1_sort_attr.c_str());
          int val2 = atof(s2_sort_attr.c_str());
          if (val1 < val2) {
            return -1;
          }
          if (val1 > val2) {
            return +1;
          }
        } else {
          // Handles string attributes
          if (s1_sort_attr.compare(s2_sort_attr) < 0){
            return -1;
          }
          if (s1_sort_attr.compare(s2_sort_attr) > 0) {
            return +1;
          }
        }
      }

      return 0;
    }

    void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    void FindShortSuccessor(std::string*) const {}

};

CustomComparator::CustomComparator(Schema *schema) {
  schema = schema;
}
// }

/**
 * Creates LevelDB database using custom comparator.
 * If there exists a database named as the file system directory, raise an error.
 */
void create_db(leveldb::DB *db, const char* directory, Schema *schema); 

/**
 * Scans through the records and insert them one by one into the leveldb index.
 */
void insert_leveldb(char *in_filename, leveldb::DB *db, Schema *schema);