# Will serializing a function automatically grab the closure?
# I highly doubt it.

some_global_var = 10

f1 = function(x) x + 100
f2 = function(x) x + 100 + some_global_var

length(serialize(f1, NULL))

f2s = serialize(f2, NULL)
length(f2s)

rm(some_global_var)

f2us = unserialize(f2s)

# Yep, fails to find `some_global_var`
# f2us(20)

# But SparkR's cleanClosure ought to take care of this.

some_global_var = 10

source("../R/utils.R")

some_global_var = 10
f2c = cleanClosure(f2)
# Note the difference between f2 and f2c ; f2c includes an environment.
# So can we directly serialize, reserialize, and use?
f2cus = unserialize(serialize(f2c, NULL))
rm(some_global_var)
# Indeed we can.
f2cus(5)


# Where else have I seen this happen? In higher order functions.
factory = function(){
    local_var = 500
    function(x) x + local_var
}

f3 = factory()
