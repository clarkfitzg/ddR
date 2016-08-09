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

# This call is necessary to set up all the optional parameters for the
# backend. Because real systems won't always use master = 'local'

#debugonce(dlist)

a = dlist(1:10)

b = collect(a)

a2 = dlist(1:10, letters, runif(10))
