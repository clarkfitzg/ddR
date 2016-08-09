library(testthat)
# Starting with the simplest possible things

library(sparkapi.ddR)
useBackend(Spark)

# A little easier to have these in the global environment for development
#library(sparkapi)
#source("../R/utils.R")
#source("../R/rddlist.R")
#source("../R/object.R")
#source("../R/driver.R")

#library(ddR)

options(error = recover)

a = dlist(1:10)
b = collect(a)

# So we need to nest
expect_equal(list(1:10), b)

a2 = dlist(1:10, letters, runif(10))

#debugonce(collect)
b2 = collect(a2)

do_collect(a2, 1L)
