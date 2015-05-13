---
title: "distributedR.dds examples"
author: "Edward Ma"
date: "2015-05-12"
output: rmarkdown::html_vignette
vignette: >
  %\VignetteIndexEntry{Vignette Title}
  %\VignetteEngine{knitr::rmarkdown}
  \usepackage[utf8]{inputenc}
---

Quick examples using distributedR.dds with the API

Starting it up:

```r
library(distributedR.dds)
useBackend(distributedR)
```

```
## Master address:port - 127.0.0.1:50002
```

Init'ing a DList:

```r
a <- dlist(nparts=5)
a <- mapply(function(x) { list(3) }, parts(a))
collect(a)
```

```
## [[1]]
## [1] 3
## 
## [[2]]
## [1] 3
## 
## [[3]]
## [1] 3
## 
## [[4]]
## [1] 3
## 
## [[5]]
## [1] 3
```

Note that we had to use `parts(a)` instead of just `a` for now. Also, we needed to do an `mapply` to initialize data inside of a. Since this is distributed R, which has strict type safety, we had to make sure each dlist partitition got a list, so we couldn't just return `3`, but `list(3)`.

Some other operations:

Adding 1 to first partition of `a`, 2 to the second, etc.

```r
b <- mapply(function(x,y) { list(x[[1]] + y ) }, parts(a), y = as.list(1:5))
collect(b)
```

```
## [[1]]
## [1] 4
## 
## [[2]]
## [1] 5
## 
## [[3]]
## [1] 6
## 
## [[4]]
## [1] 7
## 
## [[5]]
## [1] 8
```

Adding `a` to `b`, then subtracting a constant value

```r
addThenSubtract <- function(x,y,z) {
  list(x[[1]] + y[[1]] - z)
}
c <- mapply(addThenSubtract,parts(a),parts(b),MoreArgs=list(z=5))
collect(c)
```

```
## [[1]]
## [1] 2
## 
## [[2]]
## [1] 3
## 
## [[3]]
## [1] 4
## 
## [[4]]
## [1] 5
## 
## [[5]]
## [1] 6
```