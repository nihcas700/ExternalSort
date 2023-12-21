This project contains the implementations of in-memory and diskbased sorting algorithms and compares their runtimes across various optimizations. The code was run on a machine with 8-core CPU with 32 GB RAM.

# In-memory Merge sort

[This package](https://github.com/nihcas700/ExternalSort/tree/master/src/main/java/inmemory) contains the following implementations

1. Sequential Merge sort - using standard divide and conquer
2. Parallel Merge sort - using ForkJoinPool framework

Both the algorithms are run on a list of 10^8 integers, and their runtimes are as follows :-

```

| Number of integers      | Sequential Merge Sort | Parallel Merge Sort   |
| ------------------      | --------------------- | --------------------- |
| 10^8                    | 100188 millis         | 46251 millis          |

```

As can be seen, `Parallel Merge Sort` takes around half the time of `Sequential Merge Sort`.

# External Sort

[This package](https://github.com/nihcas700/ExternalSort/tree/master/src/main/java/diskbased) contains the following implementations

1. External Sort with 2-way merge and blocking IO
2. External Sort with 2-way merge and async IO

The algorithms are run on an input file containing 10^9 integers, and their runtimes are as follows :-

```

| Number of integers | 2-way merge and blocking IO | 2-way merge and async IO   |
| ------------------ | --------------------------- | -------------------------- |
| 10^9               | To be updated               | To be updated              |

```
