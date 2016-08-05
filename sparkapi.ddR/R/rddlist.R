# sparkapi provides all the invoke functions
#' @importFrom sparkapi invoke invoke_new invoke_static

CACHE_DEFAULT = TRUE

# Use S4 for consistency with ddR
setOldClass("spark_jobj")
setOldClass("spark_connection")
setClass("rddlist", slots = list(sc = "spark_connection",
                                 pairRDD = "spark_jobj",
                                 nparts = "integer",
                                 classTag = "spark_jobj"))


rddlist = function(sc, data, nparts=NULL){

    if(class(data) == "rddlist"){
        return(data)
    }

    if(!is.list(data)){
        data = list(data)
    }

    Rlist = data

    n = length(Rlist)
    if(is.null(nparts)) nparts = n
    if(nparts > n) stop("Use a smaller number of partitions.")

    # Strategy is to have about the same number of list elements in each
    # element of the RDD. This makes sense if the list elements are roughly the
    # same size.
    part_index = sort(rep(seq(nparts), length.out = n))

    parts = split(Rlist, part_index)
    serial_parts = lapply(parts, serialize, connection = NULL)

    # An RDD of the serialized R parts
    # This is class org.apache.spark.api.java.JavaRDD
    RDD = invoke_static(sc,
                        "org.apache.spark.api.r.RRDD",
                        "createRDDFromArray",
                        sparkapi::java_context(sc),
                        serial_parts)

    # This is all written specifically for bytes, so should be fine to let this 
    # classTag have a slot.
    classTag = invoke(RDD, "classTag")  # Array[byte]

    # (data, integer) pairs
    backwards = invoke(RDD, "zipWithIndex")

    # An RDD of integers
    index = invoke(backwards, "values")

    # The pairRDD of (integer, data) 
    pairRDD = invoke(index, "zip", RDD)

    new("rddlist", sc = sc, pairRDD = pairRDD, nparts = nparts,
        classTag = classTag)
}


# TODO: What exactly is the initialize method doing? Do I need it?
#setMethod("initialize", "rddlist",
#function(.Object, sc, Rlist, nparts){
#        
#    n = length(Rlist)
#    nparts = max(nparts, n)
#
#    # Strategy is to have about the same number of list elements in each
#    # element of the RDD. This makes sense if the list elements are roughly the
#    # same size.
#    part_index = sort(rep(seq(nparts), length.out = n))
#
#    parts = split(x, part_index)
#    serial_parts = lapply(parts, serialize, connection = NULL)
#
#    # An RDD of the serialized R parts
#    # This is class org.apache.spark.api.java.JavaRDD
#    RDD = invoke_static(sc,
#                        "org.apache.spark.api.r.RRDD",
#                        "createRDDFromArray",
#                        sparkapi::java_context(sc),
#                        serial_parts)
#
#    # (data, integer) pairs
#    backwards = invoke(RDD, "zipWithIndex")
#
#    # An RDD of integers
#    index = invoke(backwards, "values")
#
#    # The pairRDD of (integer, data) 
#    pairRDD = invoke(index, "zip", RDD)
#
#    .Object@sc = sc
#    .Object@pairRDD = pairRDD
#    .Object@nparts = nparts
#    # This is all written specifically for bytes, so should be fine to let this 
#    # classTag have a slot.
#    .Object@classTag = invoke(RDD, "classTag")  # Array[byte]
#    .Object
#})
#

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

partitionFunc <- function(partIndex, part) {
    zip = TRUE
    serializerMode = "byte"
    len <- length(part)
    if (len > 0) {
      if (serializerMode == "byte") {
        lengthOfValues <- part[[len]]
        lengthOfKeys <- part[[len - lengthOfValues]]
        stopifnot(len == lengthOfKeys + lengthOfValues)

        # For zip operation, check if corresponding partitions
        # of both RDDs have the same number of elements.
        if (zip && lengthOfKeys != lengthOfValues) {
          stop(paste("Can only zip RDDs with same number of elements",
                     "in each pair of corresponding partitions."))
        }

        if (lengthOfKeys > 1) {
          keys <- part[1 : (lengthOfKeys - 1)]
        } else {
          keys <- list()
        }
        if (lengthOfValues > 1) {
          values <- part[ (lengthOfKeys + 1) : (len - 1) ]
        } else {
          values <- list()
        }

        if (!zip) {
          return(mergeCompactLists(keys, values))
        }
      } else {
        keys <- part[c(TRUE, FALSE)]
        values <- part[c(FALSE, TRUE)]
      }
      mapply(
        function(k, v) { list(k, v) },
        keys,
        values,
        SIMPLIFY = FALSE,
        USE.NAMES = FALSE)
    } else {
      part
    }
  }


partitionFunc <- function(partIndex, part) {
      mapply(
        function(k, v) { list(k, v) },
        head(part),
        tail(part),
        SIMPLIFY = FALSE,
        USE.NAMES = FALSE)
  }


zip2 = function(a, b){
    aval = invoke(a@pairRDD, "values")
    bval = invoke(b@pairRDD, "values")
    # class org.apache.spark.api.java.JavaPairRDD
    zipped = invoke(aval, "zip", bval)
    # class org.apache.spark.rdd.ZippedPartitionsRDD2
    # This has the same number of elements as the input a.
    # Converting to rdd seems necessary for the invoke_new below
    RDD = invoke(zipped, "rdd")
    FUN = function(x) x
    FUN_applied = function(partIndex, part) {
        lapply(part, FUN)
    }
    FUN_clean = cleanClosure(FUN_applied)
    # Copying logic from lapply - come back and refactor
    packageNamesArr <- serialize(NULL, NULL)
    broadcastArr <- list()
    # Same length
    pairs <- invoke_new(a@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       RDD,
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       a@classTag
                       )
    # Same length
    JavaRDD = invoke(pairs, "asJavaRDD")
    # Reuse the old index to create the PairRDD
    index = invoke(a@pairRDD, "keys")
    # Same length
    pairRDD = invoke(index, "zip", JavaRDD)
    output = a
    output@pairRDD = pairRDD
    output
}


# Zips rdds together. For rdd's a, b, c:
# 
# zip(a, b, c) -> rdd[ list(a1, b1, c1), ... , list(an, bn, cn) ]
#
# Follows R's recycling rules so that the final length will be the length
# of the largest rdd
ziprdd = function(...){

}


# do_mapply(driver, func, ..., MoreArgs = list(), output.type = "dlist",
# nparts = NULL, combine = "default")
# do_mapply will call this function
# driver not necessary since that's carried around in the spark context
#
# Set cache = TRUE to use Spark to cache the result the first time it's
# computed. This could be a problem as the data pushes the limits of system
# memory.
#
mapply_rdd_list = function(func, ..., MoreArgs = list(),
            output.type = "dlist", nparts = NULL, combine = "default",
            cache = CACHE_DEFAULT)
{
    dots = list(...)
    #browser()
    # TODO: remove this constraint
    if(length(dots) > 1) stop("multiple arguments not yet supported")
    x = dots[[1]]

    # The function should be in a particular form for calling Spark's
    # org.apache.spark.api.r.RRDD class constructor
    func_applied = function(partIndex, part) {
        #lapply(part, func)
        do.call(lapply, c(list(part, func), MoreArgs))
    }
    func_clean = cleanClosure(func_applied)

    # TODO: Could come back and implement this functionality later
    packageNamesArr <- serialize(NULL, NULL)
    broadcastArr <- list()
    # I believe broadcastArr holds these broadcast variables:
    # https://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables
    # But what's the relation between broadcast variables, func's closure,
    # and the ... argument?
    
    vals = invoke(x@pairRDD, "values")

    # Use Spark to apply func
    fxrdd <- invoke_new(x@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       invoke(vals, "rdd"),
                       serialize(func_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       x@classTag
                       )

    # Convert this into class org.apache.spark.api.java.JavaRDD so we can
    # zip
    JavaRDD = invoke(fxrdd, "asJavaRDD")

    # Reuse the old index to create the PairRDD
    index = invoke(x@pairRDD, "keys")
    pairRDD = invoke(index, "zip", JavaRDD)

    if(cache) invoke(pairRDD, "cache")
   
    output = x
    output@pairRDD = pairRDD
    output
}


setMethod("[[", signature(x = "rddlist", i = "numeric", j = "missing"),
function(x, i, j){
    javaindex = i - 1L
    jlist = invoke(x@pairRDD, "lookup", javaindex)
    convertJListToRList(jlist, flatten=FALSE)
})


# Define it this way for the moment so it doesn't conflict with
# ddR::collect
# Convert the distributed list to a local list
collect_rddlist = function(rddlist){
    values = invoke(rddlist@pairRDD, "values")
    collected = invoke(values, "collect")
    convertJListToRList(collected, flatten = FALSE)
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

    xrdd = rddlist(sc, x, nparts = 2L)

    fxrdd = lapply(xrdd, FUN)

}

if(FALSE){
    x2 = collect_rddlist(xrdd)

    fx2 = collect_rddlist(fxrdd)

    xrdd[[2]]

    fxrdd[[2]]

    # Is it possible to pipeline? Yes
    ############################################################
    FUN2 = function(x) rep(x, 2)
    xrdd = new("rddlist", sc, x, nparts = 2L)
    fxrdd = lapply(xrdd, FUN)
    ffxrdd = lapply(fxrdd, FUN2)
    
    # Yes! Works!
    collect_rddlist(ffxrdd)

    # Is it lazy? Yes
    ############################################################
    hard1 = function(x){
        # Making it sleep for 3 seconds results in failed job:
        # Accept timed out
        Sys.sleep(1)
        as.character(x)
    }

    xrdd = new("rddlist", sc, x, nparts = 2L)

    # These return immediately
    fxrdd = lapply(xrdd, hard1)
    ffxrdd = lapply(fxrdd, hard1)

    # Takes time for this one => lazy!
    #collect_rddlist(ffxrdd)
    
    # Does it cache results? Not by default, but easy to turn on
    ############################################################

    # Running this multiple times returns quickly every time
    xrdd[[1]]

    # This takes several seconds every time. Even though conceptually it
    # only needed to happen once. 
    #ffxrdd[[1]]

    # Explicitly cache this
    invoke(ffxrdd@pairRDD, "cache")
    
    # Now this is slow the first time, and fast afterwards. Sweet!
    # ffxrdd[[1]]

    # Does it avoid unnecessary computation? No
    ############################################################
    chars_kill_me = function(x){
        if(is.character(x)) stop("failing miserably")
        x
    }

    xrdd = new("rddlist", sc, x, nparts = 3L)
    fxrdd = lapply(xrdd, chars_kill_me)

    # To evaluate this it's only necessary to call this function on the
    # first chunk. However, this produces an error message, implying it was
    # called on the 2nd chunk as well.
    #fxrdd[[1]]

    # This is fine- it's a much lower priority to have this.

    # Thinking more - this issue is really in the JavaPairRDD.lookup
    # method. It only needs to compute the value for the selected keys. So a fix
    # should ideally happen upstream at that level.

    # Testing mapply
    ############################################################

    x = list(c(1:10, NA), c(rnorm(20), NA))

    # The behavior to emulate
    mapply(sum, x, MoreArgs = list(na.rm = TRUE))

    xrdd = new("rddlist", sc, x, nparts = 2L)
    fxrdd = mapply_rdd_list(sum, xrdd, MoreArgs = list(na.rm = TRUE))

    out = collect_rddlist(fxrdd)

    # Verify caching behavior
    fxrdd = mapply_rdd_list(hard1, xrdd)

    # Works. This is fast the 2nd time.
    collect_rddlist(fxrdd)

}

if(TRUE){

    # Zipped RDD's

    # Here's what we want to happen in local R
    a = list(1, rnorm(1))
    b = list(1:2, rnorm(2))
    c = list(1:3, rnorm(3))

    # This is the type of operation to emulate:
    out1 = mapply(sum, a, b, c)

    # If it's zipped then we can do this with lapply. Which was the whole
    # point to make it work with Spark.
    # A nested list ready for do.call
    zipped = Map(list, a, b, c)
    out2 = lapply(zipped, function(x) do.call(sum, x))

    # Now with RDD's
    ar = rddlist(sc, a)
    br = rddlist(sc, b)
    cr = rddlist(sc, c)

    #debugonce(zip2)

    z = zip2(ar, br)
    zc = collect_rddlist(z)

    vals = invoke(z@pairRDD, "values")

    z2 = zip2(z, cr)

    unserialize(invoke(invoke(z2@pairRDD, "values"), "first"))

    #debugonce(convertJListToRList)
    zc2 = collect_rddlist(z2)

    # Works fine
    wrappedsum = function(arglist) do.call(sum, arglist)
    out3 = lapply(zc2, wrappedsum)

    # Fails    
    outrdd = lapply(z2, wrappedsum)
    out4 = collect_rddlist(outrdd)

    collect_rddlist(lapply(z2, class))
   
}
