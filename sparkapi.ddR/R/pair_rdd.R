setOldClass("spark_jobj")
setOldClass("spark_connection")
setClass("rddlist", slots = list(sc = "spark_connection",
                                 jobj = "spark_jobj", nparts = "integer"))


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

    # Original serialized data as an RRDD, which is an RDD capable of creating
    # R processes
    .Object@jobj = invoke_static(sc,
                                 "org.apache.spark.api.r.RRDD",
                                 "createRDDFromArray",
                                 java_context(sc),
                                 serial_parts)
    .Object@sc = sc
    .Object@nparts = nparts
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

    # Use Spark to apply FUN
    fxrdd <- invoke_new(X@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       invoke(X@jobj, "rdd"),
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       invoke(X@jobj, "classTag")  # Array[byte]
                       )
    
    output = X
    output@jobj = fxrdd
    output
})


# Define it this way for the moment so it doesn't conflict with
# ddR::collect
# Convert the distributed list to a local list
collect_rddlist = function(rddlist){
    collected = invoke(rddlist@jobj, "collect")
    convertJListToRList(collected, flatten = TRUE)
}

if(TRUE){

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

    #fx2 = collect_rddlist(fxrdd)

    #fxrdd[[2]]

    # Is it possible to pipeline?

}
