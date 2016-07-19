# Tue Jul 19 13:38:23 PDT 2016
#
# An attempt to write the simplest possible Spark interface
# on top of sparkapi.
#
# For real applications we'll need to be loading data.

source('rdd_utils.R')

#' Load an R list into Spark
#' @param sc Spark Context
#' @param locallist A local R list
#' @param numSlices Number of partitions to distribute the data
#' @return Reference to a Java RDD
#
# TODO: numSlices should default to number of workers available
spark_parallelize <- function(sc, locallist, numSlices = 2) {

    N <- length(locallist)

    if (numSlices > N){
        numSlices <- N
    }

    groups <- rep(seq_len(numSlices), length.out = N)
    groups <- sort(groups)

    slices <- split(locallist, groups)

    # Serialize each slice: obtain a list of raws, or a list of lists (slices) of
    # 2-tuples of raws
    serializedSlices <- lapply(slices, serialize, connection = NULL)
    
    jrdd <- invoke_static(sc,
                          "org.apache.spark.api.r.RRDD",
                          "createRDDFromArray",
                          # TODO: will these be public in sparkapi?
                          sparkapi:::java_context(sc),
                          serializedSlices)
    
    return(jrdd)
}


#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RDD.
#'
#' @param rdd The RDD to collect
#' @param flatten FALSE if the list should not flattened
#' @return a list containing elements in the RDD
spark_collect <- function(rdd, flatten = TRUE) {
  # Assumes a pairwise RDD is backed by a JavaPairRDD.
  collected <- invoke(rdd, "collect")
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
spark_lapply <- function(X, FUN) {
  UseMethod("spark_lapply")
}

spark_lapply.spark_rdd <- function(X, FUN) {
  func <- function(partIndex, part) {
    lapply(part, FUN)
  }
  spark_lapply_partitions_with_index(X, func)
}

spark_lapply_partitions_with_index <- function(X, FUN) {
  spark_rdd_pipelined_init(X, FUN)
}

# Example Code
############################################################

sc <- sparkapi::start_shell(master = "local")

rdd <- spark_parallelize(sc, 1:10, 2L)

newrdd <- spark_lapply(rdd, function(x) x + 1000)

spark_collect(newrdd)

stop_shell(sc)
