# Mon Jul 25 08:52:46 PDT 2016

# Goal is to reduce this to the minimum set of code necessary to have an
# RDD as an ordered list of binary objects which we can apply functions to.
#
# Restrictions: 
# 
#   - Pure functions => No need to process closures
#   - No pipelined RDD's => eager evaluation
#   - Only using byte arrays, ie. storing serialized R objects






# Imported from:
#    https://github.com/apache/spark/blob/v1.6.2/R/pkg/R/context.R
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# context.R: SparkContext driven functions

#' Create an RDD from a homogeneous list or vector.
#'
#' This function creates an RDD from a local homogeneous list in R. The elements
#' in the list are split into \code{numSlices} slices and distributed to nodes
#' in the cluster.
#'
#' @export
#'
#' @param sc SparkContext to use
#' @param x list to parallelize
#' @param numSlices number of partitions to create in the RDD
#' @return an RDD created from this collection
parallelize <- function(sc, x, numSlices = 1) {

  if (!is.list(x)){
        stop("x should be a list")
    }
 
  if (numSlices > length(x))
    numSlices <- length(x)
  
  sliceLen <- ceiling(length(x) / numSlices)
  slices <- split(x, rep(1: (numSlices + 1), each = sliceLen)[1:length(x)])
  
  # A list of serialized objects
  serializedSlices <- lapply(slices, serialize, connection = NULL)
  
  jrdd <- invoke_static(sc,
                        "org.apache.spark.api.r.RRDD",
                        "createRDDFromArray",
                        # TODO (Clark) will this be made public?
                        sparkapi:::java_context(sc),
                        serializedSlices)
  
  new("RDD", jrdd)
}




# Imported from:
#    https://github.com/apache/spark/blob/v1.6.2/R/pkg/R/RDD.R
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

setOldClass("spark_jobj")

#' S4 class that represents an RDD
#'
#' RDD can be created using functions like
#'              \code{parallelize}, \code{textFile} etc.
#'
#' @rdname RDD
#' @seealso parallelize, textFile
#' @slot env An R environment that stores bookkeeping states of the RDD
#' @slot jrdd Java object reference to the backing JavaRDD
#' to an RDD
#' @noRd
setClass("RDD",
         slots = list(env = "environment",
                      jrdd = "spark_jobj"))

setMethod("initialize", "RDD",
function(.Object, jrdd, 
         isCached = FALSE, isCheckpointed = FALSE) {

  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  .Object@env$isCheckpointed <- isCheckpointed
  .Object@jrdd <- jrdd
  .Object
})


setMethod("[", "RDD",
function(.Object, index){

    obj <- invoke(jList, "get", as.integer(index))

}

#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RDD.
#'
#' @export
#'
#' @param x The RDD to collect
#' @param ... Other optional arguments to collect
#' @param flatten FALSE if the list should not flattened
#' @return a list containing elements in the RDD
spark_collect <- function(x, flatten = TRUE) {
  # Assumes a pairwise RDD is backed by a JavaPairRDD.
  collected <- invoke(x@jrdd, "collect")
  convertJListToRList(collected, flatten)
}

#' Apply a function to all elements
#'
#' This function creates a new RDD by applying the given transformation to all
#' elements of the given RDD
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RDD created by the transformation.
#' @rdname lapply
#' @noRd
#' @aliases lapply
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- lapply(rdd, function(x) { x * 2 })
#' collect(multiplyByTwo) # 2,4,6...
#'}
setMethod("lapply",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            func <- function(partIndex, part) {
              lapply(part, FUN)
            }
            lapplyPartitionsWithIndex(X, func)
          })

setGeneric("lapplyPartition", function(X, FUN, ...) { standardGeneric("lapplyPartition") })
#' Apply a function to each partition of an RDD
#'
#' Return a new RDD by applying a function to each partition of this RDD.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each partition.
#' @return a new RDD created by the transformation.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' partitionSum <- lapplyPartition(rdd, function(part) { Reduce("+", part) })
#' collect(partitionSum) # 15, 40
#'}
#' @rdname lapplyPartition
#' @aliases lapplyPartition,RDD,function-method
#' @noRd
setMethod("lapplyPartition",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, function(s, part) { FUN(part) })
          })

setGeneric("lapplyPartitionsWithIndex", function(X, FUN, ...) { standardGeneric("lapplyPartitionsWithIndex") })
#' Return a new RDD by applying a function to each partition of this RDD, while
#' tracking the index of the original partition.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each partition; takes the partition
#'        index and a list of elements in the particular partition.
#' @return a new RDD created by the transformation.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 5L)
#' prod <- lapplyPartitionsWithIndex(rdd, function(partIndex, part) {
#'                                          partIndex * Reduce("+", part) })
#' collect(prod, flatten = FALSE) # 0, 7, 22, 45, 76
#'}
#' @rdname lapplyPartitionsWithIndex
#' @aliases lapplyPartitionsWithIndex,RDD,function-method
#' @noRd
setMethod("lapplyPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            new("PipelinedRDD", X, FUN, NULL)
          })

# Imported from:
#    https://github.com/apache/spark/blob/v1.6.2/R/pkg/R/utils.R
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utilities and Helpers

# Given a JList<T>, returns an R list containing the same elements, the number
# of which is optionally upper bounded by `logicalUpperBound` (by default,
# return all elements).  Takes care of deserializations and type conversions.
convertJListToRList <- function(jList, flatten, logicalUpperBound = NULL,
                                serializedMode = "byte") {
  arrSize <- invoke(jList, "size")
  
  # Datasets with serializedMode == "string" (such as an RDD directly generated by textFile()):
  # each partition is not dense-packed into one Array[Byte], and `arrSize`
  # here corresponds to number of logical elements. Thus we can prune here.
  if (serializedMode == "string" && !is.null(logicalUpperBound)) {
    arrSize <- min(arrSize, logicalUpperBound)
  }
  
  results <- if (arrSize > 0) {
    lapply(0 : (arrSize - 1),
           function(index) {
             obj <- invoke(jList, "get", as.integer(index))
             
             # Assume it is either an R object or a Java obj ref.
             if (inherits(obj, "spark_jobj")) {
               if (isInstanceOf(obj, "scala.Tuple2")) {
                 # JavaPairRDD[Array[Byte], Array[Byte]].
                 
                 keyBytes <- invoke(obj, "_1")
                 valBytes <- invoke(obj, "_2")
                 res <- list(unserialize(keyBytes),
                             unserialize(valBytes))
               } else {
                 stop(paste("utils.R: convertJListToRList only supports",
                            "RDD[Array[Byte]] and",
                            "JavaPairRDD[Array[Byte], Array[Byte]] for now"))
               }
             } else {
               if (inherits(obj, "raw")) {
                 if (serializedMode == "byte") {
                   # RDD[Array[Byte]]. `obj` is a whole partition.
                   res <- unserialize(obj)
                   # For serialized datasets, `obj` (and `rRaw`) here corresponds to
                   # one whole partition dense-packed together. We deserialize the
                   # whole partition first, then cap the number of elements to be returned.
                 } else if (serializedMode == "row") {
                   res <- readRowList(obj)
                   # For DataFrames that have been converted to RRDDs, we call readRowList
                   # which will read in each row of the RRDD as a list and deserialize
                   # each element.
                   flatten <<- FALSE
                   # Use global assignment to change the flatten flag. This means
                   # we don't have to worry about the default argument in other functions
                   # e.g. collect
                 }
                 # TODO: is it possible to distinguish element boundary so that we can
                 # unserialize only what we need?
                 if (!is.null(logicalUpperBound)) {
                   res <- head(res, n = logicalUpperBound)
                 }
               } else {
                 # obj is of a primitive Java type, is simplified to R's
                 # corresponding type.
                 res <- list(obj)
               }
             }
             res
           })
  } else {
    list()
  }
  
  if (flatten) {
    as.list(unlist(results, recursive = FALSE))
  } else {
    as.list(results)
  }
}

