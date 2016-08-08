
if(FALSE){
    x2 = collect_rdd(xrdd)

    fx2 = collect_rdd(fxrdd)

    xrdd[[2]]

    fxrdd[[2]]

    # Is it possible to pipeline? Yes
    ############################################################
    FUN2 = function(x) rep(x, 2)
    xrdd = new("rddlist", sc, x, nparts = 2L)
    fxrdd = lapply(xrdd, FUN)
    ffxrdd = lapply(fxrdd, FUN2)
    
    # Yes! Works!
    collect_rdd(ffxrdd)

    # Is it lazy? Yes
    ############################################################
    hard1 = function(x){
        # Making it sleep for 3 seconds results in failed job:
        # Accept timed out
        Sys.sleep(1)
        as.character(x)
    }

    xrdd = new("rddlist", sc, x, nparts = 2L)

    # These return immediately
    fxrdd = lapply(xrdd, hard1)
    ffxrdd = lapply(fxrdd, hard1)

    # Takes time for this one => lazy!
    #collect_rdd(ffxrdd)
    
    # Does it cache results? Not by default, but easy to turn on
    ############################################################

    # Running this multiple times returns quickly every time
    xrdd[[1]]

    # This takes several seconds every time. Even though conceptually it
    # only needed to happen once. 
    #ffxrdd[[1]]

    # Explicitly cache this
    invoke(ffxrdd@pairRDD, "cache")
    
    # Now this is slow the first time, and fast afterwards. Sweet!
    # ffxrdd[[1]]

    # Does it avoid unnecessary computation? No
    ############################################################
    chars_kill_me = function(x){
        if(is.character(x)) stop("failing miserably")
        x
    }

    xrdd = new("rddlist", sc, x, nparts = 3L)
    fxrdd = lapply(xrdd, chars_kill_me)

    # To evaluate this it's only necessary to call this function on the
    # first chunk. However, this produces an error message, implying it was
    # called on the 2nd chunk as well.
    #fxrdd[[1]]

    # This is fine- it's a much lower priority to have this.

    # Thinking more - this issue is really in the JavaPairRDD.lookup
    # method. It only needs to compute the value for the selected keys. So a fix
    # should ideally happen upstream at that level.

    # Testing mapply
    ############################################################

    x = list(c(1:10, NA), c(rnorm(20), NA))

    # The behavior to emulate
    mapply(sum, x, MoreArgs = list(na.rm = TRUE))

    xrdd = new("rddlist", sc, x, nparts = 2L)
    fxrdd = mapply_rdd_list(sum, xrdd, MoreArgs = list(na.rm = TRUE))

    out = collect_rdd(fxrdd)

    # Verify caching behavior
    fxrdd = mapply_rdd_list(hard1, xrdd)

    # Works. This is fast the 2nd time.
    collect_rdd(fxrdd)


