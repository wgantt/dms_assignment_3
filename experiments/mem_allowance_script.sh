#!/bin/bash

MEM=(572 1144 2288 4576 9152 18304 36608 73216 146432 292864)
INPUT="test_records/test_records_1M.csv"
OUTPUT="mem_allowance.out"
for m in ${MEM[*]}; do
	echo "Timing exeuction for mem_capacity = $m..." >> $OUTPUT
	{ time ./msort test_schema.json $INPUT $m-$file.out $m 10 cgpa; } 2>> $OUTPUT
	echo "Finished timing execution for mem_capacity = $m." >> $OUTPUT
done
