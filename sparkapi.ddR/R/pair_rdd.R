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
# TODO: support dots
# function(X, FUN, ...){
function(X, FUN, ...){

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

browser()
    
    # Converts to ParallelCollectionRDD
    # TODO: Is this step necessary?
    xrdd = invoke(X@jobj, "rdd")

#Wed Jul 27 13:36:59 PDT 2016 - This is failing
    # Now apply the function
    fxrdd <- invoke_new(X@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       xrdd,
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       invoke(xrdd, "classTag")
                       )
})


# Define it this way for the moment so it doesn't conflict with
# ddR::collect
# Convert the distributed list to a local list
collect_rddlist = function(rddlist){
    collected = invoke(rddlist@jobj, "collect")
    convertJListToRList(collected)
}



############################################################

if(FALSE){

# This works and gives a SeqWrapper
#collected <- invoke(rdd, "collect")
#convertJListToRList(collected, flatten=TRUE)
#convertJListToRList(collected, flatten=FALSE)

# The function should be in a particular form. I don't see any documentation
# for this.
#cleanfunc = cleanClosure(func)

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

fxrdd_java <- invoke(fxrdd, "asJavaRDD")
collected <- invoke(fxrdd_java, "collect")

# It's possible here to grab the first element. So why do we need the
# previous two steps converting to JavaRDD and collecting?
first = invoke(fxrdd, "first")
unserialize(first)

# SparkR seems to be relying on this RDD maintaining the same order here.
final = convertJListToRList(collected, flatten=TRUE)

# Now for part 1) How to retreive elements? x[i]

# The PairwiseRRDD seems like it may be useful, but I don't think it maps
# functions to data.
#
# Looked at the implementation for R's PairRDD- seems inefficient.
# For a key lookup you have to evaluate _everything_ and get the key from
# R. A different approach is to keep a lookup table in local R session
# mapping keys to integers, and then use the integers to look up the
# appropriate values

# Expect 2
invoke(fxrdd, "count")

# class org.apache.spark.rdd.ZippedWithIndexRDD
# This doesn't help at all because it zips with the index as the value, so
# I can't use it to look things up.
backwards_zipped = invoke(fxrdd, "zipWithIndex")

# The first key value pair
z1 = invoke(backwards_zipped, "first")

invoke(z1, "_2")
invoke(z1, "_1")

invoke(backwards_zipped, "first")

# Following this
# http://stackoverflow.com/questions/26828815/how-to-get-element-by-index-in-spark-rdd-java

# Neither work
# zipped = invoke(backwards_zipped, "map", "_.swap")
# zipped = invoke(backwards_zipped, "map", "case (k,v) => (v,k)")

#(0 to 100).toList

bigint = invoke_new(sc, "java.math.BigInteger", "100000000")

index = invoke_new(sc, "java.util.ArrayList")
invoke(index, "add", 1L)
invoke(index, "add", 2L)

# This works, which means I can pass in arguments
#invoke(index, "add", bigint)

# Maybe this fails since it needs to be a scala collection
# index_rdd = invoke(sc$spark_context, "parallelize", index)

########################################
# Let's try going from the Java RDD

# Gives (data, integer) pairs
javazip_backwards = invoke(fxrdd_java, "zipWithIndex")

# This is now an RDD of integers
index = invoke(javazip_backwards, "values")

# The pairRDD of (integer, data) 
javazip = invoke(index, "zip", fxrdd_java)

# produces a Scala tuple
jz1 = invoke(javazip, "first")
# Wonderful- no longer backwards
invoke(jz1, "_2")

# Produces a Scala sequence
jz1_lookup = invoke(javazip, "lookup", 1L)
jz1_val = convertJListToRList(jz1_lookup, flatten=TRUE)

# So could write this as a method
do_collect = function(pairRDD, Rindex){
    javaindex = Rindex - 1
    seq = invoke(pairRDD, "lookup", javaindex)
    convertJListToRList(seq, flatten=TRUE)
}

do_collect(javazip, 1L)
}

if(FALSE){

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

    fxrdd[[2]]

    # Is it possible to pipeline?

}
