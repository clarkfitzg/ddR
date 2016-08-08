# Perform zip or cartesian between elements from two RDDs in each partition
# param
#   rdd An RDD.
#   zip A boolean flag indicating this call is for zip operation or not.
# return value
#   A result RDD.
mergePartitions <- function(rdd, zip) {
  serializerMode <- getSerializedMode(rdd)
  partitionFunc <- function(partIndex, part) {
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

  PipelinedRDD(rdd, partitionFunc)
}

# Perform zip or cartesian between elements from two RDDs in each partition
# param
#   rdd An RDD.
#   zip A boolean flag indicating this call is for zip operation or not.
# return value
#   A result RDD.
mergePartitions2 <- function(rdd, zip=TRUE) {
  serializerMode <- "byte"
  partitionFunc <- function(partIndex, part) {
    len <- length(part)
    if (len > 0) {
      if (serializerMode == "byte") {
        lengthOfValues <- 1
        lengthOfKeys <- 1
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

    FUN = partitionFunc
    FUN_applied = function(partIndex, part) {
        lapply(part, FUN)
    }
    FUN_clean = cleanClosure(FUN_applied)

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
    pairs
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


zip2_mergePartitions = function(a, b){
    aval = invoke(a@pairRDD, "values")
    bval = invoke(b@pairRDD, "values")
    # class org.apache.spark.api.java.JavaPairRDD
    zipped = invoke(aval, "zip", bval)
    # class org.apache.spark.rdd.ZippedPartitionsRDD2
    # This has the same number of elements as the input a.
    # Converting to rdd seems necessary for the invoke_new below
    RDD = invoke(zipped, "rdd")
    partitionFunc <- function(partIndex, part) {
        keys <- part[[1]]
        values <- part[[length(part)]]
        mapply(
          function(k, v) { list(k, v) },
          keys,
          values,
          SIMPLIFY = FALSE,
          USE.NAMES = FALSE)
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


zip3 = function(a, b){
    aval = invoke(a@pairRDD, "values")
    bval = invoke(b@pairRDD, "values")
    # class org.apache.spark.api.java.JavaPairRDD
    zipped = invoke(aval, "zip", bval)
    # class org.apache.spark.rdd.ZippedPartitionsRDD2
    # This has the same number of elements as the input a.
    # Converting to rdd seems necessary for the invoke_new below
    RDD = invoke(zipped, "rdd")
    partitionFunc <- function(partIndex, part) {
        c(length=length(part), class=class(part))
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


zip4 = function(a, b){
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


lapply2 = function(X, FUN){
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
}


if(TRUE){

    source('rddlist.R')

    # Zipped RDD's

    # Here's what we want to happen in local R
    set.seed(37)
    a = list(1:10, rnorm(5), rnorm(3))
    b = list(21:30, rnorm(5), rnorm(3))
    c = list(1:3, rnorm(3))

    # This is the type of operation to emulate:
    out1 = mapply(sum, a, b)

    # If it's zipped then we can do this with lapply. Which was the whole
    # point to make it work with Spark.
    # A nested list ready for do.call
    zipped = Map(list, a, b)
    out2 = lapply(zipped, function(x) do.call(sum, x))

    # Now with RDD's
    ar = rddlist(sc, a)
    br = rddlist(sc, b)
    cr = rddlist(sc, c)

}

if(FALSE){
    # Mon Aug  8 09:44:56 KST 2016
    # Carefully checking everything out

    set.seed(37)
    a = list(1:10, rnorm(5), rnorm(3))
    b = list(21:30, rnorm(5), rnorm(3))
    ar = rddlist(sc, a)
    br = rddlist(sc, b)
 
    z = zip2_mergePartitions(ar, br)

    # A list of three lists, which was the goal
    zc = collect_rddlist(z)

    # This is different than what we had before - in an encouraging way.
    # It actually seems to have done something closer to the zip we wanted.
    collect_rddlist(lapply(z, class))

    # From before:
    collect_rddlist(lapply(zip2(ar, br), class))

    # Continuing to iterate on this:
    collect_rddlist(zip3(ar, br))

    # This shows that each "part" is a list of length 2. That's
    # straightforward.

    z4 = zip4(ar, br)

    # Works and gives the result that I wanted.
    collect_rddlist(lapply2(z4, class))

    # OTOH, this we'd like to be (int, num, num) instead of list
    collect_rddlist(lapply2(ar, class))

    out4 = lapply2(z4, function(x) do.call(sum, x))

    # Matches out1 and out2 above. Yes!!
    out4 = collect_rddlist(out4)

}

if(FALSE){

    v = invoke(ar@pairRDD, "values")

    invoke(v, "rdd")

    # These are MapPartitionsRDD. Any relation to what mergePartition does?

}


if(FALSE){
    #debugonce(zip2)

    z = zip2(ar, br)
    zc = collect_rddlist(z)

    collect_rddlist(lapply(z, class))

    # No help
    # collect_rddlist(lapply(lapply(z, list), class))

    vals = invoke(z@pairRDD, "values")

    z2 = zip2(z, cr)

    unserialize(invoke(invoke(z2@pairRDD, "values"), "first"))

    #debugonce(convertJListToRList)
    zc2 = collect_rddlist(z2)

    # These should be the same:
    all.equal(zc2[[1]], z2[[1]])

    # Works fine
    wrappedsum = function(arglist) do.call(sum, arglist)
    out3 = lapply(zc2, wrappedsum)

    # Fails    
    outrdd = lapply(z2, wrappedsum)
    #out4 = collect_rddlist(outrdd)

    # two lists, which is what I want
    lapply(zc2, class)

    # two lists containing the classes of all the elements
    # => applying the function happened inside the lists.
    collect_rddlist(lapply(z2, class))

    # What if we force a collect, and go from there?
    # Idea: If we have the raw bytes back we can create it again using the
    # static method.
    invoke(z2@pairRDD, "collect")

    # First let's check if it works when it's given the right list in the
    # first place.
    withlists = rddlist(sc, zc2)

    # seems to have gotten another layer of nesting in this process.
    # But this is not a problem, I can always deal with it.
    collect_rddlist(withlists)
    
    # Comparing this with the above we see the nesting in the 2nd and 3rd
    # layer is different
    collect_rddlist(lapply(z2, list))

    # This returns two "list", correct except for the extra layer of
    # nesting.
    out = collect_rddlist(lapply(withlists, class))
    out
   
    collect_rddlist(lapply(z2, class))

    collect_rddlist(lapply(z2, function(x) x))

    # class scala.collection.convert.Wrappers$SeqWrapper
    # Where are the docs for it? I want to see all available methods
    # Documentation: 
    zbytes = invoke(invoke(z2@pairRDD, "values"), "collect")

    # Works - I believe this is a Java method
    invoke(zbytes, "size")
    # Fails - Scala method of a sequence
    #invoke(zbytes, "length")

    # Sat Aug  6 09:56:22 KST 2016
    # Hypothesis: zbytes is a mysterious Java object 
    # I need to convert it into a sequence of byte arrays.
    zbyte_class = invoke(zbytes, "getClass")

    # Aha- see all the methods
    invoke(zbyte_class, "getMethods")

    # These are the bytes I expected.
    invoke(zbytes, "get", 1L)

    # Pretty cool- returns the thing straight back to R:
    underlying = invoke(zbytes, "underlying")

    # This is therefore a much simpler way to convert from Jlist to Rlist.
    lapply(underlying, unserialize)

    # This appears to be doing the same thing as `underlying`.
    invoke(zbytes, "toArray")

    # Is it possible to convert this to a more tractable object?
    # What it really needs to be is whatever lapply(zc2, serialize, connection = NULL))
    # (R list of bytes) is converted to in Java,
    # because that's what is working for the constructor.

    # Fails
    # invoke_new(sc, "ArrayList", zbytes)

    # Fails
    # invoke_new(sc, "ArrayList", invoke(zbytes, "iterator"))

    # This looks correct, just like zc2 above
    convertJListToRList(zbytes, flatten=FALSE)

    # Can we convert from R back to something like zbytes? Yes.
    zbytes2 = invoke_static(sc,
                        "org.apache.spark.api.r.RRDD",
                        "createRDDFromArray",
                        sparkapi::java_context(sc),
                        lapply(zc2, serialize, connection = NULL))

    zbytes2 = invoke(zbytes2, "collect")

    invoke(zbytes2, "get", 1L)

    # Works, this is the same as zc2.
    convertJListToRList(zbytes2, flatten=FALSE)


    # From the docs the last arg here should be a sequence of byte arrays.
    # http://spark.apache.org/docs/latest/api/java/org/apache/spark/api/r/RRDD.html#createRDDFromArray(org.apache.spark.api.java.JavaSparkContext,%20byte[][])
    # It surprises me that zbytes is something different.

    # Fails
#    RDD = invoke_static(sc,
#                        "org.apache.spark.api.r.RRDD",
#                        "createRDDFromArray",
#                        sparkapi::java_context(sc),
#                        zbytes)

    # Edward's approach doesn't seem to work- wouldn't expect it to since
    # I've modified so much.
#    compound.RDD <- lapply(z2, function(x) list(x[[1]][[1]], lapply(x, 
#            function(y) y[[2]])))
#    collect_rddlist(compound.RDD)
    
# The problem may be in the nesting... so maybe the solution is to remove
# the nesting by converting back into bytes.
}
