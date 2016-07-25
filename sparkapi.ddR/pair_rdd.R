# Mon Jul 25 10:14:43 PDT 2016

# We basically need to do two things:
# 1) [ Retrieve elements 
# 2) Map R functions onto binary data

library(sparkapi)

# This is all I want spark to do:
############################################################
x = list(1:10, letters)
func = function(x) x[1:5]
fx = lapply(x, func)
############################################################


parts = split(x, 2)
serial_parts <- lapply(parts, serialize, connection = NULL)

sc <- start_shell(master = "local")

# Original serialized data as an RRDD, which is an RDD capable of creating
# R processes
rdd <- invoke_static(sc,
                      "org.apache.spark.api.r.RRDD",
                      "createRDDFromArray",
                      # TODO (Clark) will this be made public?
                      sparkapi:::java_context(sc),
                      serial_parts)


# Not exactly sure why this is necessary
# invoke(rdd, "rdd"),

# Don't know what this is
# invoke(rdd, "classTag")

packageNamesArr <- serialize("base", NULL)
broadcastArr <- list()

# Now apply the function
frdd <- invoke_new(sc,
                     "org.apache.spark.api.r.RRDD",  # A new instance of this class
                     invoke(rdd, "rdd"),  # Converts to ParallelCollectionRDD
                     serialize(func, NULL),
                     "byte",  # name of serializer / deserializer
                     "byte",  # name of serializer / deserializer
                     packageNamesArr,  
                     broadcastArr,
                     invoke(rdd, "classTag")
                   )

frdd2 <- invoke(frdd, "asJavaRDD")

collected <- invoke(frdd2, "collect")
