#!/bin/bash

OUTPUT="time2.out"
for file in test_records/*; do
	echo "Timing execution of $file..." >> $OUTPUT
	{ time ./msort test_schema.json $file $file.out 26260 100 cgpa; } 2>> $OUTPUT
	echo "Finished timing $file." >> $OUTPUT
done
