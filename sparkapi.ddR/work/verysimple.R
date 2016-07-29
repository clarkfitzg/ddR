# Starting with the simplest possible things

library(sparkapi.ddR)
#library(ddR)

# This call is necessary to set up all the optional parameters for the
# backend. Because real systems won't always use master = 'local'
useBackend(Spark)

a = dlist(1:10, letters)

b = collect(a)
