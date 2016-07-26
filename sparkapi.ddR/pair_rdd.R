# Mon Jul 25 10:14:43 PDT 2016

# We basically need to do two things:
# 1) [ Retrieve elements 
# 2) Map R functions onto binary data

library(sparkapi)

# For the second task this is all I want spark to do:
############################################################
x = list(1:10, letters, rnorm(10))
FUN = function(x) x[1:5]
fx = lapply(x, FUN)
############################################################

nparts = 2
# TODO: generalize this splitting
parts = split(x, c(1, 1, 2))
serial_parts <- lapply(parts, serialize, connection = NULL)

sc <- start_shell(master = "local")

# Original serialized data as an RRDD, which is an RDD capable of creating
# R processes
xrdd <- invoke_static(sc,
                      "org.apache.spark.api.r.RRDD",
                      "createRDDFromArray",
                      java_context(sc),
                      serial_parts)

# This works and gives a SeqWrapper
#collected <- invoke(rdd, "collect")
#convertJListToRList(collected, flatten=TRUE)
#convertJListToRList(collected, flatten=FALSE)

# The function should be in a particular form. I don't see any documentation
# for this.
#cleanfunc = cleanClosure(func)
# This gets us cleanClosure and convertJListToRList
source('R/rdd_utils.R')

FUN_withapply <- function(partIndex, part) {
  lapply(part, FUN)
}
FUN_clean = cleanClosure(FUN_withapply)

# Not exactly sure why this is necessary
# invoke(rdd, "rdd"),

# Don't know what this is
# invoke(rdd, "classTag")

# This works when it's NULL
packageNamesArr <- serialize(NULL, NULL)
broadcastArr <- list()

# Now apply the function
fxrdd <- invoke_new(sc,
                   "org.apache.spark.api.r.RRDD",  # A new instance of this class
                   invoke(xrdd, "rdd"),  # Converts to ParallelCollectionRDD
                   serialize(FUN_clean, NULL),
                   "byte",  # name of serializer / deserializer
                   "byte",  # name of serializer / deserializer
                   packageNamesArr,  
                   broadcastArr,
                   invoke(xrdd, "classTag")
                   )

fxrdd2 <- invoke(fxrdd, "asJavaRDD")

collected <- invoke(fxrdd2, "collect")

final = convertJListToRList(collected, flatten=TRUE)

# Now for part 1) How to retreive elements? x[i]

# Following this
# http://stackoverflow.com/questions/26828815/how-to-get-element-by-index-in-spark-rdd-java
#backwards_zipped = invoke(fxrdd, "zipWithIndex")

#zipped = invoke(backwards_zipped, "map", "case (k,v) => (v,k)")

#invoke_new(sc, "java.math.BigInteger", "1000000000")

#invoke_new(sc, "scala.collection.immutable.Range.Inclusive", 

#invoke_static(sc, "IntStream", "rangeClosed", 1, 10)

# TODO: how to use things like '1:10' in Scala?
# May have to ask Javier on this.
