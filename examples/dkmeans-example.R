library(ddR)
library(kmeans.ddR)

set.seed(319804)

# Define level of parallelism
N_EXEC = 2 

# Set up data size
NCOL = 8 # Must be at least 2
NROW = 2e5L
K = 6
ITERMAX = 300

# Set the backend
useBackend(parallel, executors = N_EXEC)

# Or uncomment the following lines to use Distributed R 
#library(distributedR.ddR)
#useBackend(distributedR)

# The true centers for the data generating process
# Easy to check correctness if we know where these centers should be
centers = cbind(10 * seq.int(K), matrix(0, nrow = K, ncol = NCOL - 1))
dnumrows = as.integer(NROW/N_EXEC)

# This function will run on the distributed backend.
# Parallelism is generally easier if this is a "pure" function,
# meaning it doesn't depend on global state.
generateKMeansData <- function(id, centers, nrow, ncol, k) {
    offsets = matrix(rnorm(nrow * ncol), nrow = nrow, ncol = ncol)
    cluster_ids = sample.int(k, size = nrow, replace = TRUE)
    feature_obs = centers[cluster_ids, ] + offsets
    feature_obs
}

cat(sprintf("Generating %d x %d matrix for clustering with %d means\n",
            NROW, NCOL, K))

dfeature <- dmapply(generateKMeansData, id = 1:N_EXEC,
  MoreArgs = list(centers = centers, nrow = dnumrows, ncol = NCOL, k = K),
		output.type = "darray", 
		combine = "rbind", nparts = c(N_EXEC,1))

# Parallelism has overhead, and depending on the problem this may or
# may not be faster than a single threaded version. But generally parallel
# algorithms scale better to larger data.
cat("training dkmeans model on distributed data\n")
dtraining_time <- system.time(
    dmodel <- dkmeans(dfeature, K, iter.max = ITERMAX)
)[3]
cat(dtraining_time, "\n")

# Collecting makes the distributed object into a local object
feature <- collect(dfeature)

cat("training kmeans model on centralized data\n")
training_time <- system.time(
    model <- kmeans(feature, K, iter.max = ITERMAX, algorithm = "Lloyd")
)[3]
cat(training_time, "\n")

# Compare results
sortfirstcol = function(m) m[order(m[, 1]), ]

# A list of three matrices with the coordinates of the cluster centers
# represented as rows
allcenters = lapply(list(true = centers, 
                         distributed = dmodel$centers,
                         centralized = model$centers),
                    sortfirstcol)

print(allcenters)

# Shutdown the backend because distributed computation is finished
shutdown(parallel)

plot(
