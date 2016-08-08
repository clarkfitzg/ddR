# Mon Aug  8 11:35:07 KST 2016
# Experiments to understand exactly how the class constructor for
# org.apache.spark.api.r.RRDD works, which will show what the programming
# model should be.

library(sparkapi)

setwd('~/dev/ddR/sparkapi.ddR/R')
source('rddlist.R')

# Testing
FUN = function(x) x[1:5]

# local R way
x = list(1:10, letters, rnorm(10))
fx = lapply(x, FUN)

# Spark RDD way
sc <- start_shell(master = "local")

xrdd = rddlist(sc, x, nparts = 2L)


