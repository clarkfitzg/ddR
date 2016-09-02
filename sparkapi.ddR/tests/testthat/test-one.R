# Run it like so to test one individually:

# Rscript test-one.R /Users/clark/dev/ddR/ddR/tests/testthat/test-darray.R

# Seems like they're mostly failing in Spark with message:
#       unused argument (v = 0)
#


# file for testing common functionality between ddR and another backend
library(testthat)
library(sparkapi.ddR)

driver_name = "spark"

dr = useBackend(driver_name)

fname <- commandArgs(trailingOnly = TRUE)

context(paste("testing", fname))

# Some tests rely on this explicitly
ddR.env <- ddR:::ddR.env

test_file(fname)

shutdown()
