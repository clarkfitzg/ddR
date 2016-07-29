################################################################### Copyright 2015 Hewlett-Packard Development Company, L.P.  This program is free
################################################################### software; you can redistribute it and/or modify it under the terms of the GNU
################################################################### General Public License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA.

#' @import methods ddR 
#' @importFrom sparkapi start_shell stop_shell



# Create distributedR ddRDriver
setClass("SparkddR", contains = "ddRDriver")

#' @export 
# Exported Driver
Spark <- new("SparkddR", DListClass = "ddR_RDD", DFrameClass = "ddR_RDD",
    DArrayClass = "ddR_RDD", backendName = "Spark")

# Environmental variables
sparkapi.ddR.env = new.env(emptyenv())
# Unsure what this should be doing.
#ddR.env$driver = Spark

#' @export
setMethod("init", "SparkddR", function(x, ...) {
    message("Backend switched to Spark. Initializing the Spark context...")
    
    dots = list(...)
    if (is.null(dots[["master"]])){
        message("Using default value master = 'local'")
        dots[["master"]] <- "local"
    }

    sc = start_shell(master = dots[["master"]], ...)
    sparkapi.ddR.env$sc = sc

    # This is a list with available memory for each executor
    memory_status = invoke(sc$spark_context, "getExecutorMemoryStatus")
    # TODO verify this works. A bit tricky with the various Spark launching
    # options
    nexecutors = length(memory_status)
    nexecutors
})

#' @export
setMethod("shutdown", "SparkddR", function(x) {
    message("Stopping the Spark shell...")
    stop_shell(Spark.ddR.env$sc)
})

#' @export
setMethod("do_dmapply",
    signature(driver = "SparkddR", func = "function"),
function(driver, func, ..., MoreArgs = list(),
    output.type = c("dlist", "dframe", "darray", "sparse_darray"),
    nparts = NULL, combine = c("default", "c", "rbind", "cbind")){
   
    # Convert ... into distributed objects if they're not already.
    rddlist = 

    ## Last step: Create new ddR_RDD object
    
    #new("ddR_RDD", RDD = output.RDD, nparts = nparts,
        #psize = psizes, dim = dims, partitions = 1:prod(nparts))

})
