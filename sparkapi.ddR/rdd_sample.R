# Mon Jul 18 16:17:47 PDT 2016

library(sparkapi)

source('R/rdd.R')
source('R/rdd_context.R')
source('R/rdd_utils.R')

sc <- start_shell(master = "local")

rdd <- spark_parallelize(sc, 1:10, 2L)

newrdd <- spark_lapply(rdd, function(x) x + 1000)

# Works
spark_collect(newrdd)

weird_thing <- list(list(mean, sum, max), 1:10)

n <- 3
a <- spark_parallelize(sc, weird_thing, 2L)
a2 <- spark_lapply(a, function(x) x[1:n])
a3 <- spark_collect(a2)

# I'm surprised that this works. It detects the global variable n and sends
# it over to Spark. And it handles an arbitrary R object like a function.
# Powerful.

stop_shell(sc)
