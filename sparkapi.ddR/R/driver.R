# Copyright 2015 Hewlett-Packard Development Company, L.P.  This program is free
# software; you can redistribute it and/or modify it under the terms of the GNU
# General Public License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA.

#' @import methods ddR 

setOldClass("spark_connection")

# Create distributedR ddRDriver
setClass("Spark.ddR", contains = "ddRDriver",
        slots = c(sc = "spark_connection"))


init_spark <- function(master = "local", ...) {
    message("Backend switched to Spark. Initializing the Spark context...")
    
    sc = sparkapi::start_shell(master, ...)

    # This is a list with available memory for each executor
    memory_status = invoke(sc$spark_context, "getExecutorMemoryStatus")
    # TODO verify this works. A bit tricky with the various Spark launching
    # options
    executors = length(memory_status)

    new("Spark.ddR",
        DListClass = "ParallelObj",
        DFrameClass = "ParallelObj",
        DArrayClass = "ParallelObj",
        name = "spark",
        executors = executors,
        sc = sc
        )
}


#' @export
setMethod("shutdown", "Spark.ddR", function(x) {
    message("Stopping the Spark shell...")
    sparkapi::stop_shell(x@sc)
})


Rlist_to_dlist = function(Rlist, nparts){

    # Strategy is to have about the same number of list elements in each
    # element of the RDD. This makes sense if the list elements are roughly
    # the same size.
    part_index = sort(rep(seq(nparts), length.out = length(Rlist)))

    parts = split(Rlist, part_index)
    partlist = lapply(parts, list)

    rdd = rddlist(sparkapi.ddR.env$sc, partlist)

    new("ParallelObj", RDD = rdd, nparts = nparts, psize = psizes, dim = dims, 
        partitions = 1:prod(nparts))

}

#' @export
setMethod("do_dmapply",
    signature(driver = "Spark.ddR", func = "function"),
function(driver, func, ..., MoreArgs = list(),
    output.type = c("dlist", "dframe", "darray", "sparse_darray"),
    nparts = NULL, combine = c("default", "c", "rbind", "cbind")){

    # Edward's steps
    # ====================
    ## 1: Convert all non-distributed object inputs into RDDs 
    ## 2: Repartition all distributed RDD inputs to have the same number of
    ##    partitions as the output
    ## 3: Zip up all inputs into one RDD
    ## 4: Insert wrapper functions, list-of-parts conversion
    ## 5: Run lapplyWithPartitions
    ## 6: Collect and compute psizes
    ## 7: Create new ParallelObj object


#    dots = list(...)
#    rdds = lapply(dots, rddlist, sc = driver@sc)
#    mapply_args = c(list(func), rdds)
#    output_rdd = do.call(mapply_rdd, mapply_args)
#
#    output_length = length_rdd(output_rdd)
#
#    # TODO: temporarily hardcoding all this in to get simple thing working.
#    # This assumes a list with the same number of partitions as it's length,
#    # like an rddlist
#    if(output.type == "dlist"){
#        dims = output_length
#        psizes = matrix(rep(1L, output_length), ncol=1L)
#        nparts = c(output_length, 1L)
#    }
#
#    new("ParallelObj", RDD = output_rdd, nparts = nparts, psize = psizes, dim = dims, 
#        partitions = 1:prod(nparts))

# Clark: Why is this code here rather than in the dmapply function?
  stopifnot(is.list(MoreArgs))
  output.type <- match.arg(output.type)
  if (!is.null(nparts)) {
      stopifnot(is.numeric(nparts),
                length(nparts) == 1L || length(nparts) == 2L)
  }
  combine <- match.arg(combine)

  dots <- list(...)
  dlen<-length(dots)


  for(num in 1:dlen){
    if(is(dots[[num]],"DObject")){
      #If this is a DObject, we need to extract the backend object and reassemble it (i.e., use collect)
      #By converting DObject into a normal R object, we let mcmapply handle how iterations on differnt data types
      dots[[num]] <- collect(dots[[num]])
    }else{
      #There are two cases for a list (1) parts(dobj) or (2) list of parts(dobj)

      if(is(dots[[num]],"list") && any(rapply(dots[[num]], function(x) is(x,"ParallelObj"),how="unlist"))){
        tmp <- rapply(dots[[num]],function(argument){
	    	      if(is(argument,"DObject"))
            	         return(argument@pObj[argument@splits])
	              else return(argument)}, how="replace")

       #(iR): This is bit of a hack. rapply increases the depth of the list, but at the level that the replacement occured.
       #Simple ulist() does not work. If this was just parts(A,..), we can call unlist. If this is a list of parts, then
       #we unwrap the second layer of the list. Unwrapping the second layer is incorrect if parts(A) was embedded deeper than that.
       if(is(dots[[num]][[1]], "DObject")){
       	  dots[[num]] <- unlist(tmp, recursive=FALSE)
       } else {
          for(index in seq_along(tmp)){
       	        if(length(tmp[[index]])>0)
			tmp[[index]] <- unlist(tmp[[index]], recursive=FALSE)
	        else
			tmp[[index]] <- tmp[[index]]
 	       }
          dots[[num]] <- tmp
      }
     }
    }
   }

   #Check if MoreArgs contains a distributed object. If yes, convert it into a regular object via collect
   for(index in seq_along(MoreArgs)){
         if(is(MoreArgs[[index]], "DObject"))
      	   MoreArgs[[index]] <- collect(MoreArgs[[index]])
   }

    answer <- NULL
    # Now iterate in parallel

############################################################

# Original code with rddlist:
#    dots = list(...)
#    rdds = lapply(dots, rddlist, sc = driver@sc)
#    mapply_args = c(list(func), rdds)
#    output_rdd = do.call(mapply_rdd, mapply_args)

# Parallel code:
#    # Wrap the input arguments and use do.call()
#    allargs <- c(list(cl = driver@cluster,
#                      fun = func,
#                      MoreArgs = MoreArgs,
#                      RECYCLE = FALSE, SIMPLIFY = FALSE),
#                 dots)
#    answer <- do.call(parallel::clusterMap, allargs)

# New code
# TODO: support MoreArgs
rdds <- lapply(dots, rddlist, sc = driver@sc)
mapply_args <- c(list(func), rdds)
output_rdd <- do.call(mapply_rdd, mapply_args)
answer <- collect_rdd(output_rdd)

############################################################

   #Perform a cheap check on whether there was an error since man pages say that an error on one core will result in error messages on all. TODO: Sometimes the class of the error is "character"
   if(inherits(answer[[1]], "try-error")) {stop(answer[[1]])}
   if(class(answer[[1]]) == "character" && grepl("Error", answer[[1]])) {stop(answer[[1]])}

   #Create the output object, since we store partitions
   totalParts <- prod(nparts)
   outputObj<-vector("list", totalParts)

   #Create the output based on the number of partitions expected in the output
   #We create partitions with near equal sizes, i.e. if the #elements is not purely divisible by totalparts, we spread the remainder
   #to the 1:remainder elements
   lenAnswer<-length(answer)
   if(totalParts > lenAnswer) stop("Number of elements generated by dmapply is less than those supplied in argument 'nparts'.")

   remainder<-lenAnswer %% totalParts
   elemInEachPart<- rep(floor(lenAnswer/totalParts), totalParts)
   if(remainder>0) {elemInEachPart[1:remainder]<-elemInEachPart[1:remainder]+1}
   elemInEachPart<-c(0,cumsum(elemInEachPart))

   #Decide how we may need to combine entries from the answer into partitions
   #We handle the "flatten" case in the while loop since simplify2array has to be called with parameter "higher=FALSE"
   combineFunc <- list

   if(output.type !="dlist"){
       #Setup the partition types that we will use later to check if partitions conform to output.type.
       if(output.type == "darray") ptype<-"matrix"
       if(output.type == "dframe") ptype<-"data.frame"
       if(output.type == "sparse_darray") ptype<-c("dsCMatrix", "dgCMatrix")

	if(combine == "rbind"){
	   if(ddR.env$RminorVersion > 2) #If R >3.2, use new rbind
	   	   combineFunc <- rbind
           else
	   	   combineFunc <- Matrix::rBind
	}
	else if(combine == "cbind"){
	   if(ddR.env$RminorVersion > 2) #If R >3.2, use new cbind
	   	   combineFunc <- cbind
           else
	   	   combineFunc <- Matrix::cBind
       }
   }
   index<-1
   psizes<-array(0L,dim=c(totalParts,2)) #Stores partition sizes
   while(index <= totalParts){
   	     if(output.type == "dlist"){
	        if(combine == "c")
			     outputObj[[index]] <- unlist(answer[(elemInEachPart[index]+1):elemInEachPart[index+1]], recursive=FALSE)
	        else
			     outputObj[[index]] <- answer[(elemInEachPart[index]+1):elemInEachPart[index+1]]
             }else {
	        if(combine == "c" || combine =="default"){
	        	     outputObj[[index]] <- simplify2array(answer[(elemInEachPart[index]+1):elemInEachPart[index+1]], higher=FALSE)
                }else{
			     outputObj[[index]] <- do.call(combineFunc, answer[(elemInEachPart[index]+1):elemInEachPart[index+1]])
                }
	        if(!(class(outputObj[[index]]) %in% ptype)) {stop("Each partition of the result should be of type = ", ptype, ", to match with output.type =", output.type)}
	     }

	     d<-dim(outputObj[[index]])
	     psizes[index,] <-(if(is.null(d)){c(length(outputObj[[index]]),0L)} else {d})
	     index<-index+1
   }

   dims<-NULL

   #Check if partions conform and can be stitched together.
   #Check partitions in each logical row have the same number of rows/height. Similary for columns
   rowseq<-seq(1, totalParts, by=nparts[2])
   for (index in rowseq){
	     if(any(psizes[index:(index+nparts[2]-1),1]!=psizes[index,1])) stop("Adjacent partitions have different number of rows, should be ", psizes[index,1])
   }

   for (index in 1:nparts[2]){
	    if(any(psizes[(rowseq+(index-1)),2]!=psizes[index,2])) stop("Adjacent partitions have different number of columns, should be ", psizes[index,2])
   }

  numcols<-sum(psizes[1:nparts[2],2]) #add cols of all partitions in the first row
  numrows<-sum(psizes[seq(1, totalParts, by=nparts[2]), 1]) #add all partitions in the first column group
  dims<-c(numrows, numcols)


  #Use single dimension if we know it's a list
  if(output.type=="dlist"){
	 psizes<-as.matrix(psizes[,1])
	 dims<-dims[1]
   }

   new("ParallelObj",pObj = outputObj, splits = 1:length(outputObj), psize = psizes, dim = as.integer(dims), nparts = nparts)

})
