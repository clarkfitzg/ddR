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


rddlist = function(sc, data){

    if(class(data) == "rddlist"){
        return(data)
    }

    if(!is.list(data)){
        stop("data should be a list")
    }

    serial_parts = lapply(data, serialize, connection = NULL)

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


frombytes = function(bytes){
    data = unserialize(bytes)
    if( length(data) != 1 ){
        stop("This function does not support nested data")
    }
    data[[1]]
}


setMethod("[[", signature(x = "rddlist", i = "numeric", j = "missing"),
function(x, i, j){
    javaindex = as.integer(i - 1L)
    jlist = invoke(x@pairRDD, "lookup", javaindex)
    bytes = invoke(collected, "toArray")
    frombytes(bytes)
    #browser()
})


# Now relying on sparkapi to do the translation
collect_rddlist = function(rddlist){
    values = invoke(rddlist@pairRDD, "values")
    collected = invoke(values, "collect")
    rawlist = invoke(collected, "toArray")
    lapply(rawlist, frombytes)
}


if(TRUE){
# Basic tests for rddlist

library(sparkapi)
library(testthat)

# This gets us cleanClosure
source('utils.R')

sc <- start_shell(master = "local")

x = list(1:10, letters, rnorm(10))
xrdd = rddlist(sc, x)

############################################################

test_that("round trip serialization", 
    expect_identical(x, collect_rddlist(xrdd))
)

test_that("simple indexing",
    i = 1
    expect_identical(x[[i]], xrdd[[i]])
)

test_that("zip RDD's",
    set.seed(37)
    a = list(1:10, rnorm(5), rnorm(3))
    b = list(21:30, rnorm(5), rnorm(3))

    ar = rddlist(sc, a)
    br = rddlist(sc, b)

    zipped = Map(list, a, b)
    zipped_rdd = zip_rdd(ar, br)

    expect_identical(zipped, collect_rddlist(zipped_rdd))
)

       

test_that("lapply",
    # TODO Come back to this later since mapply is the priority.
    first5 = function(x) x[1:5]
    fx = lapply(x, first5)
    fx = lapply(x, first5)
    #fxrdd = 
)

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

