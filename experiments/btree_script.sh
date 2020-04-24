#!/bin/bash

DIR="../leveldb_dir"
OUTPUT1="bsort.out"
OUTPUT2="msort_poor.out"
OUTPUT3="msort_good.out"

for file in test_records/*; do
	if [ -d "$DIR" ]; then rm -Rf $DIR; fi

	echo "Timing execution of $file..." >> $OUTPUT1
	{ time ./bsort test_schema.json $file $file.out cgpa; } 2>> $OUTPUT1
	echo "Finished timing $file." >> $OUTPUT1

	echo "Timing execution of $file..." >> $OUTPUT2
	{ time ./msort test_schema.json $file $file.out 572 2 cgpa; } 2>> $OUTPUT2
	echo "Finished timing $file." >> $OUTPUT2

	echo "Timing execution of $file..." >> $OUTPUT3
	{ time ./msort test_schema.json $file $file.out 28600 100 cgpa; } 2>> $OUTPUT3
	echo "Finished timing $file." >> $OUTPUT3
	
done