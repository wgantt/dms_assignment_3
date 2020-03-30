import csv    # for writing the output CSV
import string # for random string attribute generation
import random # for random string attribute generation
import numpy as np   # for rounding operations in random sampling

'''
You should implement this script to generate test data for your
merge sort program.

The schema definition should be separate from the data generation
code. See example schema file `schema_example.json`.
'''
ALPHABET = string.ascii_uppercase
ACCEPTED_DATA_TYPES = ['string', 'integer', 'float']

def sample_uniform(range_min, range_max, length, is_int=True):
  """
  Samples a random integer or float from a uniform distribution. Min and max
  are taken to be inclusive.
  """
  if is_int:
    return random.choice([i for i in range(range_min, range_max + 1)])
  else:
    return np.round(np.random.uniform(range_min, range_max), decimals=length) 

def sample_normal(mu, sigma, range_min, range_max, length, is_int=False):
  """
  Samples a random integer or float from a normal distribution
  """
  sample = np.random.normal(loc=mu, scale=sigma)

  if not is_int:
    sample = np.round(sample, decimals=length)

  # ensure sample is within [range_min, range_max]
  if sample > range_max:
    return range_max
  elif sample < range_min:
    return range_min
  else:
    return sample

def generate_data(schema, out_file, nrecords):
  '''
  Generate data according to the `schema` given,
  and write to `out_file`.
  `schema` is an list of dictionaries in the form of:
    [ 
      {
        'name' : <attribute_name>, 
        'length' : <fixed_length>,
        ...
      },
      ...
    ]
  `out_file` is the name of the output file.
  The output file must be in csv format, with a new line
  character at the end of every record.
  '''
  print("Generating %d records" % nrecords)

  with open(out_file, 'w') as out:

    # dictionary of attribute values, keyed on attribute name
    records = {}

    # generate n values for each attribute in the schema
    for attribute in schema:

      # determine type, length, and name of attribute
      attr_type = attribute['type']
      attr_name = attribute['name']
      attr_len = attribute['length']

      # verify that the type is valid
      assert (attr_type in ACCEPTED_DATA_TYPES), "Invalid attribute type."

      # if this is a string attribute, generate n random
      # strings of the appropriate length
      if attr_type == 'string':

        # this method of generating random strings was inspired by:
        # https://stackoverflow.com/questions/2030053/random-strings-in-python
        records[attr_name] = [''.join(random.choice(ALPHABET) for i in range(attr_len)) for n in range(nrecords)]

      # if it isn't a string attribute, it's a numerical attribute  
      else:

        # determine whether it's an integer
        is_int = attr_type == 'integer'

        # if a distribution is specified, sample random values according to
        # that distribution
        if 'distribution' in attribute:
          dist = attribute['distribution']
          dist_type = dist['name']

          # sample uniform
          if dist_type == 'uniform':
            records[attr_name] = [sample_uniform(dist['min'], dist['max'], attr_len, is_int=is_int) for n in range(nrecords)]

          # otherwise, assume we're dealing with a normal distribution
          else:
            records[attr_name] = [sample_normal(dist['mu'], dist['sigma'], dist['min'], dist['max'], attr_len, is_int=is_int) for n in range(nrecords)]

        # if no distribution is specified, sample a uniform distribution
        # over the interval [0,attribute_Length]
        else:
          records[attr_name] = [sample_uniform(0, attr_len, attr_len, is_int=is_int) for n in range(nrecords)]

    # initialize the output CSV with its header
    attr_names = records.keys()
    writer = csv.DictWriter(out, fieldnames=attr_names)
    writer.writeheader()

    # write the records to the output CSV
    for tup in zip(*records.values()):
      record = {attr: val for attr, val in zip(attr_names, tup)}
      writer.writerow(record)

if __name__ == '__main__':
  import sys, json
  if not len(sys.argv) == 4:
    print("data_generator.py <schema json file> <output csv file> <# of records>")
    sys.exit(2)

  schema = json.load(open(sys.argv[1]))
  output = sys.argv[2]
  nrecords = int(sys.argv[3])
  print(schema)
  
  generate_data(schema, output, nrecords)