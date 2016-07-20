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

setClass("PipelinedRDD",
         slots = list(prev = "RDD",
                      func = "function",
                      prev_jrdd = "spark_jobj"),
         contains = "RDD")

setMethod("initialize", "RDD",
function(.Object, jrdd, serializedMode = "byte",
         isCached = FALSE, isCheckpointed = FALSE) {

  # Check that RDD constructor is using the correct version of serializedMode
  stopifnot(class(serializedMode) == "character")
  stopifnot(serializedMode %in% c("byte", "string", "row"))
  # RDD has three serialization types:
  # byte: The RDD stores data serialized in R.
  # string: The RDD stores data as strings.
  # row: The RDD stores the serialized rows of a SparkDataFrame.

  # We use an environment to store mutable states inside an RDD object.
  # Note that R's call-by-value semantics makes modifying slots inside an
  # object (passed as an argument into a function, such as cache()) difficult:
  # i.e. one needs to make a copy of the RDD object and sets the new slot value
  # there.

  # The slots are inheritable from superclass. Here, both `env' and `jrdd' are
  # inherited from RDD, but only the former is used.
  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  .Object@env$isCheckpointed <- isCheckpointed
  .Object@env$serializedMode <- serializedMode

  .Object@jrdd <- jrdd
  .Object
})


setMethod("initialize", "PipelinedRDD",
function(.Object, prev = NULL, func = function(x) x, jrdd_val = NULL) {
  .Object@env <- new.env()
  .Object@env$isCached <- FALSE
  .Object@env$isCheckpointed <- FALSE
  .Object@env$jrdd_val <- jrdd_val
  if (!is.null(jrdd_val)) {
    # This tracks the serialization mode for jrdd_val
    .Object@env$serializedMode <- prev@env$serializedMode
  }

  .Object@prev <- prev

  isPipelinable <- function(rdd) {
    e <- rdd@env
    # nolint start
    !(e$isCached || e$isCheckpointed)
    # nolint end
  }

  if (!inherits(prev, "PipelinedRDD") || !isPipelinable(prev)) {
    # This transformation is the first in its stage:
    .Object@func <- cleanClosure(func)
    .Object@prev_jrdd <- getJRDD(prev)
    .Object@env$prev_serializedMode <- prev@env$serializedMode
    # NOTE: We use prev_serializedMode to track the serialization mode of prev_JRDD
    # prev_serializedMode is used during the delayed computation of JRDD in getJRDD
  } else {
    pipelinedFunc <- function(partIndex, part) {
      f <- prev@func
      func(partIndex, f(partIndex, part))
    }
    .Object@func <- cleanClosure(pipelinedFunc)
    .Object@prev_jrdd <- prev@prev_jrdd # maintain the pipeline
    # Get the serialization mode of the parent RDD
    .Object@env$prev_serializedMode <- prev@env$prev_serializedMode
  }

  .Object
})

# Return the serialization mode for an RDD.
setGeneric("getSerializedMode", function(rdd, ...) { standardGeneric("getSerializedMode") })
# For normal RDDs we can directly read the serializedMode
setMethod("getSerializedMode", signature(rdd = "RDD"), function(rdd) rdd@env$serializedMode )
# For pipelined RDDs if jrdd_val is set then serializedMode should exist
# if not we return the defaultSerialization mode of "byte" as we don't know the serialization
# mode at this point in time.
setMethod("getSerializedMode", signature(rdd = "PipelinedRDD"),
          function(rdd) {
            if (!is.null(rdd@env$jrdd_val)) {
              return(rdd@env$serializedMode)
            } else {
              return("byte")
            }
          })
# The jrdd accessor function.
setGeneric("getJRDD", function(rdd, ...) { standardGeneric("getJRDD") })
setMethod("getJRDD", signature(rdd = "RDD"), function(rdd) rdd@jrdd )
setMethod("getJRDD", signature(rdd = "PipelinedRDD"),
function(rdd, serializedMode = "byte") {



  if (!is.null(rdd@env$jrdd_val)) {
    return(rdd@env$jrdd_val)
  }
  sc <- spark_connection(rdd@prev_jrdd)
  packageNamesArr <- serialize(sc$packages,
                               connection = NULL)
  
  broadcastArr <- list()
  
  serializedFuncArr <- serialize(rdd@func, connection = NULL)
  
  prev_jrdd <- rdd@prev_jrdd
  
  if (serializedMode == "string") {
    rddRef <- invoke_new("org.apache.spark.api.r.StringRRDD",
                         invoke(prev_jrdd, "rdd"),
                         serializedFuncArr,
                         rdd@env$prev_serializedMode,
                         packageNamesArr,
                         broadcastArr,
                         invoke(prev_jrdd, "classTag"))
  } else {
    rddRef <- invoke_new("org.apache.spark.api.r.RRDD",
                         invoke(prev_jrdd, "rdd"),
                         serializedFuncArr,
                         rdd@env$prev_serializedMode,
                         serializedMode,
                         packageNamesArr,
                         broadcastArr,
                         invoke(prev_jrdd, "classTag"))
  }
  # Save the serialization flag after we create a RRDD
  rdd@env$serializedMode <- serializedMode
  rdd@env$jrdd_val <- invoke(rddRef, "asJavaRDD")
  rdd@env$jrdd_val
})

getJRDD.spark_rdd_pipelined <- function(rdd, serializedMode = "byte") {
  if (!is.null(rdd$env$jrdd_val)) {
    return(rdd$env$jrdd_val)
  }
  sc <- spark_connection(rdd$prev_jrdd)
  packageNamesArr <- serialize(sc$packages,
                               connection = NULL)
  
  broadcastArr <- list()
  
  serializedFuncArr <- serialize(rdd$func, connection = NULL)
  
  prev_jrdd <- rdd$prev_jrdd
  
  if (serializedMode == "string") {
    rddRef <- invoke_new(sc,
                         "org.apache.spark.api.r.StringRRDD",
                         invoke(prev_jrdd, "rdd"),
                         serializedFuncArr,
                         rdd$env$prev_serializedMode,
                         packageNamesArr,
                         broadcastArr,
                         invoke(prev_jrdd, "classTag"))
  } else {
    rddRef <- invoke_new(sc,
                         "org.apache.spark.api.r.RRDD",
                         invoke(prev_jrdd, "rdd"),
                         serializedFuncArr,
                         rdd$env$prev_serializedMode,
                         serializedMode,
                         packageNamesArr,
                         broadcastArr,
                         invoke(prev_jrdd, "classTag"))
  }
  # Save the serialization flag after we create a RRDD
  rdd$env$serializedMode <- serializedMode
  rdd$env$jrdd_val <- invoke(rddRef, "asJavaRDD")
  rdd$env$jrdd_val
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
  collected <- invoke(getJRDD(x), "collect")
  convertJListToRList(collected, flatten,
                      serializedMode = getSerializedMode(x))
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
