################################################################### Copyright 2015 Hewlett-Packard Development Company, L.P.  This program is free
################################################################### software; you can redistribute it and/or modify it under the terms of the GNU
################################################################### General Public License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA.

#' @include rddlist.R

# Create the ddR_RDD Object
setClass("ddR_RDD", contains = "DObject",
    slots = list(RDD = "rddlist", partitions = "numeric"), 
    prototype = prototype(nparts = c(1L, 1L), psize = matrix(1, 1),
                          dim = c(1L, 1L)))

setMethod("initialize", "ddR_RDD", function(.Object, ...) {
    # Dispatches to rddlist?
    .Object <- callNextMethod(.Object, ...)
})

#' @export
setMethod("get_parts", signature("ddR_RDD", "missing"), function(x, ...) {

    # Placeholder
    NULL
})

#' @export
setMethod("get_parts", signature("ddR_RDD", "integer"),
function(x, index, ...) {

    # Placeholder
    NULL
})

#' @export
setMethod("do_collect", signature("ddR_RDD", "integer"),
function(x, parts) {

    # Placeholder
    NULL
})

setMethod("combine", signature(driver = "ddR_RDD", items = "list"),
function(driver, items) {

    #new("ddR_RDD", RDD = items[[1]]@RDD, partitions = unlist(split_indices), dim = dims, 
        #psize = psizes)

    # Placeholder
    NULL
})
