#!/bin/bash

K=(5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100)
INPUT="test_records/test_records_1M.csv"
OUTPUT="k.out"
for k in ${K[*]}; do
	echo "Timing exeuction for k = $k..." >> $OUTPUT
	{ time ./msort test_schema.json $INPUT k-$k.out 26260 $k cgpa; } 2>> $OUTPUT
	echo "Finished timing execution for k = $k." >> $OUTPUT
done
