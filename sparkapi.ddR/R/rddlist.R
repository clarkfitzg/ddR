require("methods")
# sparkapi provides all the invoke functions
#' @importFrom sparkapi invoke invoke_new invoke_static

# TODO: Would be better to turn this on as an option in Spark
CACHE_DEFAULT = TRUE

# Use S4 for consistency with ddR
setOldClass("spark_jobj")
setOldClass("spark_connection")
setClass("rddlist", slots = list(sc = "spark_connection",
                                 pairRDD = "spark_jobj",
                                 nparts = "integer",
                                 classTag = "spark_jobj"))


# This rddlist is pretty basic. Given an R list it loads it into Spark with
# each element of the list corresponding to an element of the Spark RDD.
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

    new("rddlist", sc = sc, pairRDD = pairRDD, nparts = length(data),
        classTag = classTag)
}


setMethod("lapply", signature(X = "rddlist", FUN = "function"),
function(X, FUN){
# TODO: support dots
# function(X, FUN, ...){

    # The function should be in a particular form for calling Spark's
    # org.apache.spark.api.r.RRDD class constructor
    FUN_applied = function(partIndex, part) {
        FUN(part)
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
mapply_rddlist = function(func, ..., MoreArgs = list(),
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
    javaindex = as.integer(i - 1L)
    javabytes = invoke(x@pairRDD, "lookup", javaindex)
    # The bytes come wrapped in a list
    bytes = invoke(javabytes, "toArray")[[1]]
    unserialize(bytes)
})



# a_nested = TRUE means that a is already in the form of a nested list with
# two layers: [ [a1], [a2], ... , [an] ]
# 
# Would be better to have this function be private and zip_rdd be the
# public exported function.
#
zip2 = function(a, b, a_nested = FALSE, b_nested = FALSE){
    # They must be nested for this to work
    if(!a_nested){
        a = lapply(a, list)
    }
    if(!b_nested){
        b = lapply(b, list)
    }
    aval = invoke(a@pairRDD, "values")
    bval = invoke(b@pairRDD, "values")
    # class org.apache.spark.api.java.JavaPairRDD
    zipped = invoke(aval, "zip", bval)
    # class org.apache.spark.rdd.ZippedPartitionsRDD2
    # This has the same number of elements as the input a.
    # Converting to rdd seems necessary for the invoke_new below
    RDD = invoke(zipped, "rdd")
    partitionFunc <- function(partIndex, part) {
        part
    }
    FUN_clean = cleanClosure(partitionFunc)
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
    JavaRDD = invoke(pairs, "asJavaRDD")
    # Reuse the old index to create the PairRDD
    index = invoke(a@pairRDD, "keys")
    # Same length
    pairRDD = invoke(index, "zip", JavaRDD)
    output = a
    output@pairRDD = pairRDD
    output
}


zip_rdd = function(...){
    args = list(...)
    a = args[[1]]
    n = length(args)

    zipped = lapply(a, list)

    if(n == 1L){
        # Easy out for trivial case
        # Note zipping will always have the same nested structure
        return(zipped)
    }

    # A 'reduce' operation
    for(i in 2:n){
        zipped = zip2(zipped, args[[i]], a_nested = TRUE)
    }
    zipped
}



# Collects and unserializes from Spark back into local R.
collect_rddlist = function(rddlist){
    values = invoke(rddlist@pairRDD, "values")
    collected = invoke(values, "collect")
    rawlist = invoke(collected, "toArray")
    lapply(rawlist, unserialize)
}


if(TRUE){
# Basic tests for rddlist

library(sparkapi)
library(testthat)

# This gets us cleanClosure
source('utils.R')

if(!exists('sc')){
    sc <- start_shell(master = "local")
}

x = list(1:10, letters, rnorm(10))
xrdd = rddlist(sc, x)

############################################################

test_that("round trip serialization", {

    collected = collect_rddlist(xrdd)
    expect_identical(x, collected)

})

test_that("simple indexing", {
    i = 1
    expect_identical(x[[i]], xrdd[[i]])
})

test_that("zipping several RDD's", {

    set.seed(37)
    a = list(1:10, rnorm(5), rnorm(3))
    b = list(21:30, rnorm(5), rnorm(3))

    ar = rddlist(sc, a)
    br = rddlist(sc, b)

    abzip = Map(list, a, b)
    abzip_rdd = zip2(ar, br)

    abzip_rdd_collected = collect_rddlist(abzip_rdd)

    expect_identical(abzip, abzip_rdd_collected)

    expect_identical(abzip, collect_rddlist(zip_rdd(ar, br)))

    # Now for 3+
    c = list(101:110, rnorm(5), rnorm(7))
    cr = rddlist(sc, c)

    abczip = Map(list, a, b, c)
    abczip_rdd = zip_rdd(ar, br, cr)

    abczip_rdd_collected = collect_rddlist(abczip_rdd)

    expect_identical(abczip, abczip_rdd_collected)

})

test_that("lapply", {

    first5 = function(x) x[1:5]
    fx = lapply(x, first5)
    fxrdd = collect_rddlist(lapply(xrdd, first5))

    expect_identical(fx, fxrdd)
})

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

