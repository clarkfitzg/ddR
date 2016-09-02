# file for testing common functionality between ddR and another backend
library(testthat)
library(sparkapi.ddR)

driver_name = "spark"

useBackend(driver_name)

context("testing common features")

# Test everything except those specific to the parallel driver
# TODO: change this to a relative path
to_test <- list.files("/Users/clark/dev/ddR/ddR/tests/testthat",
                    full.names = TRUE)
to_test <- to_test[!grepl("test-pdriver.R", to_test)
                    & grepl("test.+\\.R", to_test)]

# Some tests rely on this explicitly
ddR.env <- ddR:::ddR.env

lapply(to_test, test_file)
