library(ddR)
library(kmeans.ddR)

set.seed(319804)

# Define level of parallelism:
N_EXEC <- 2 
# Dimension of the data should be at least 2:
NCOL <- 4L
# Number of observations:
NROW <- 2e5L
# Number of clusters expected:
K <- 3L
# Maximum number of iterations:
ITERMAX <- 300

# Set the backend
useBackend(parallel, executors = N_EXEC)

# Or uncomment the following lines to use Distributed R 
#library(distributedR.ddR)
#useBackend(distributedR)

# The true centers for the data generating process
# Easy to check correctness if we know where these centers should be
centers <- cbind(10 * seq.int(K), matrix(0, nrow = K, ncol = NCOL - 1))
dnumrows <- as.integer(NROW/N_EXEC)

# This function will run on the distributed backend.
# Parallelism is generally easier if this is a 'pure' function,
# meaning it doesn't depend on or modify global state.
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

# Collecting pulls the distributed object from the backend into a local object
feature <- collect(dfeature)

cat("training kmeans model on centralized data\n")
training_time <- system.time(
    model <- kmeans(feature, K, iter.max = ITERMAX, algorithm = "Lloyd")
)[3]
cat(training_time, "\n")

# Compare results
sortfirstcol <- function(m) m[order(m[, 1]), ]

# A list of three matrices with the coordinates of the cluster centers
# represented as rows
allcenters <- lapply(list(true = centers, 
                         distributed = dmodel$centers,
                         centralized = model$centers),
                    sortfirstcol)

print(allcenters)

# Plotting the results
dim1 <- 1
dim2 <- 2
plot(allcenters$true[, c(dim1, dim2)],
     xlim = range(sapply(allcenters, function(x) range(x[, dim1]))),
     ylim = range(sapply(allcenters, function(x) range(x[, dim2]))),
     main = "Two dimensions of the discovered centers",
     xlab = "First dimension", ylab = "Second dimension"
)
points(allcenters$distributed, pch = 2)
points(allcenters$centralized, pch = 3)
legend("bottomright", legend = c("true", "distributed", "centralized"),
        pch = 1:3)

# If successful the points found by both versions of kmeans should be close
# to the true centers. Note the scale of the axes may be deceptive here.

shutdown(parallel)
