#!/bin/bash

LOGFILE=results.log

rm $LOGFILE

# Run all the tests sequentially and log the output
# Note that can't have any unnecessary files in this directory
for fname in $(find /Users/clark/dev/ddR/ddR/tests/testthat -type f); do
    Rscript test-one.R $fname >> $LOGFILE
done


egrep ": [\.0-9]+" results.log 

# This shows that we're only failing 15 of 148 tests of the at the 
# moment:  Fri Sep  2 17:22:36 KST 2016
# That's encouraging.

# Failures
egrep -o ": [\.0-9]+" results.log | grep -o [0-9] | wc -l

# Total number of tests
egrep -o ": [\.0-9]+" results.log | wc -m

# This "unused argument" seems to be the first issue to fix
grep "v =" results.log
