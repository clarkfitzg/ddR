# Mon Jul 18 16:17:47 PDT 2016

library(sparkapi)

#source('R/rdd.R')
#source('R/context.R')
#source('R/utils.R')
source('R/minimal.R')

sc <- start_shell(master = "local")

rdd <- parallelize(sc, 1:10, 2L)

newrdd <- lapply(rdd, function(x) x + 1000)

# Come back to this point:
local_rdd = spark_collect(newrdd)


weird_thing <- list(list(mean, sum, max), 1:10)

n <- 3
a <- spark_parallelize(sc, weird_thing, 2L)
a2 <- spark_lapply(a, function(x) x[1:n])
a3 <- spark_collect(a2)

# I'm surprised that this works. It detects the global variable n and sends
# it over to Spark. And it handles an arbitrary R object like a function.
# Powerful.

stop_shell(sc)
