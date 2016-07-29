# Starting with the simplest possible things

library(sparkapi.ddR)
#library(ddR)

# I guess this call is necessary to set up all the optional parameters for the
# backend
useBackend(Spark)

a = dlist(1:10, letters)

b = collect(a)
