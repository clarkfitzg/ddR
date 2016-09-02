#!/bin/bash

# Run all the tests sequentially and log the output
# Note that can't have any unnecessary files in this directory
for fname in $(find /Users/clark/dev/ddR/ddR/tests/testthat -type f); do
    Rscript test-one.R $fname >> results.log
done
