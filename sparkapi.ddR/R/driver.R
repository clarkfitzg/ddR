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
Spark <- new("SparkddR", DListClass = "ddR_RDD", DFrameClass = "ddR_RDD", DArrayClass = "ddR_RDD", 
    backendName = "Spark")

#' @export
setMethod("init", "SparkddR", function(x, ...) {
    message("Backend switched to Spark. Initializing the Spark context...")
    
    dots = list(...)
    if (is.null(dots[["master"]])){
        message("Using default value master = 'local'")
        dots[["master"]] <- "local"
    }

    Spark.ddR.env$context <- start_shell(master = dots[["master"]], ...)

    ## TODO(etduwx): return the actual number of executors
    return(1)
})

#' @export
setMethod("shutdown", "SparkddR", function(x) {
    message("Stopping the Spark shell...")
    stop_shell(Spark.ddR.env$context)
})

#' @export
setMethod("do_dmapply",
    signature(driver = "SparkddR", func = "function"),
    function(driver, func, ..., MoreArgs = list(),
    output.type = c("dlist", "dframe", "darray", "sparse_darray"),
    nparts = NULL, combine = c("default", "c", "rbind", "cbind")){
   


    ## Last step: Create new ddR_RDD object
    
    new("ddR_RDD", RDD = output.RDD, nparts = nparts, psize = psizes, dim = dims, 
        partitions = 1:prod(nparts))

})
