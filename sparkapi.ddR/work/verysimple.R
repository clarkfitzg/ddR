# Starting with the simplest possible things

library(sparkapi.ddR)
#library(ddR)

# This call is necessary to set up all the optional parameters for the
# backend. Because real systems won't always use master = 'local'
useBackend(Spark)

#debugonce(dlist)
# 1) ddR::dlist
# 2) dmapply identity function on list(1:10, letters)
#Error in nparts(result) :
  #trying to get slot "nparts" from an object of a basic class ("integer")
#with no slots 
# Here nparts = c(2, 1)
a = dlist(1:10, letters)

b = collect(a)

a2 = dlist(1:10)
