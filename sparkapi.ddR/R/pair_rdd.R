
# Use S4 for consistency with ddR
setOldClass("spark_jobj")
setOldClass("spark_connection")
setClass("rddlist", slots = list(sc = "spark_connection",
                                 pairRDD = "spark_jobj",
                                 nparts = "integer",
                                 classTag = "spark_jobj"))


setMethod("initialize", "rddlist",
function(.Object, sc, Rlist, nparts){
        
    n = length(Rlist)
    nparts = max(nparts, n)

    # Strategy is to have about the same number of list elements in each
    # element of the RDD. This makes sense if the list elements are roughly the
    # same size.
    part_index = sort(rep(seq(nparts), length.out = n))

    parts = split(x, part_index)
    serial_parts = lapply(parts, serialize, connection = NULL)

    #browser()

    # An RDD of the serialized R parts
    # This is class org.apache.spark.api.java.JavaRDD
    RDD = invoke_static(sc,
                        "org.apache.spark.api.r.RRDD",
                        "createRDDFromArray",
                        java_context(sc),
                        serial_parts)

    # (data, integer) pairs
    backwards = invoke(RDD, "zipWithIndex")

    # An RDD of integers
    index = invoke(backwards, "values")

    # The pairRDD of (integer, data) 
    pairRDD = invoke(index, "zip", RDD)

    # TODO: delete
    # Is there any advantage to converting from Java object? No, won't zip
    # then.
    #rdd = invoke(RDD, "rdd")
    #pairRDD2 = invoke(index, "zip", rdd)

    .Object@sc = sc
    .Object@pairRDD = pairRDD
    .Object@nparts = nparts
    # This is all written specifically for bytes, so should be fine to let this 
    # classTag have a slot.
    .Object@classTag = invoke(RDD, "classTag")  # Array[byte]
    .Object
})


setMethod("lapply", signature(X = "rddlist", FUN = "function"),
function(X, FUN){
# TODO: support dots
# function(X, FUN, ...){

    # The function should be in a particular form for calling Spark's
    # org.apache.spark.api.r.RRDD class constructor
    FUN_applied = function(partIndex, part) {
        lapply(part, FUN)
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

#browser()

    # Convert this into class org.apache.spark.api.java.JavaRDD so we can
    # zip
    JavaRDD = invoke(fxrdd, "asJavaRDD")

    # Reuse the old index to create the PairRDD
    index = invoke(X@pairRDD, "keys")
    pairRDD = invoke(index, "zip", JavaRDD)
   
    output = X
    output@pairRDD = pairRDD
    output
})


setMethod("[[", signature(x = "rddlist", i = "numeric", j = "missing"),
function(x, i, j){
    javaindex = i - 1L
    jlist = invoke(x@pairRDD, "lookup", javaindex)
    convertJListToRList(jlist, flatten=TRUE)
})


# Define it this way for the moment so it doesn't conflict with
# ddR::collect
# Convert the distributed list to a local list
collect_rddlist = function(rddlist){
    values = invoke(rddlist@pairRDD, "values")
    collected = invoke(values, "collect")
    convertJListToRList(collected, flatten = TRUE)
}


if(TRUE){
    # Tests - could formalize these

    # This gets us cleanClosure and convertJListToRList
    source('utils.R')

    # Testing
    library(sparkapi)
    FUN = function(x) x[1:5]

    # local R way
    x = list(1:10, letters, rnorm(10))
    fx = lapply(x, FUN)
    fx[[2]]

    # Spark RDD way
    sc <- start_shell(master = "local")

    xrdd = new("rddlist", sc, x, nparts = 2L)

    fxrdd = lapply(xrdd, FUN)

    x2 = collect_rddlist(xrdd)

    fx2 = collect_rddlist(fxrdd)

    xrdd[[2]]

    fxrdd[[2]]

    # Is it possible to pipeline?

}
