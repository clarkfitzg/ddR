lapply2 = function(X, FUN){
# TODO: support dots
# function(X, FUN, ...){

    # The function should be in a particular form for calling Spark's
    # org.apache.spark.api.r.RRDD class constructor
    FUN_applied = function(partIndex, part) {
        list(partIndex = partIndex, part = FUN(part))
    }
    FUN_clean = cleanClosure(FUN_applied)

    # TODO: Could come back and implement this functionality later
    packageNamesArr <- serialize(NULL, NULL)
    broadcastArr <- list()
    # I believe broadcastArr holds these broadcast variables:
    # https://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables
    # But what's the relation between broadcast variables, FUN's closure,
    # and the ... argument?
    
    vals = invoke(X@pairRDD, "values")

    # Use Spark to apply FUN
    fxrdd <- invoke_new(X@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       invoke(vals, "rdd"),
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       X@classTag
                       )

    # Convert this into class org.apache.spark.api.java.JavaRDD so we can
    # zip
    JavaRDD = invoke(fxrdd, "asJavaRDD")

    # Reuse the old index to create the PairRDD
    index = invoke(X@pairRDD, "keys")
    pairRDD = invoke(index, "zip", JavaRDD)
   
    output = X
    output@pairRDD = pairRDD
    output
}


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

xrdd = rddlist(sc, x)

invoke(invoke(invoke(xrdd@pairRDD, "values"), "collect"), "toArray")
