This project contains the implementations of in-memory and diskbased sorting algorithms and compares their runtimes across various optimizations. 
The code was run on a machine with 8-core CPU with 32 GB RAM with macOS 14.2 .

# In-memory Merge sort

[This package](https://github.com/nihcas700/ExternalSort/tree/master/src/main/java/inmemory) contains the following implementations

1. Sequential Merge sort - using standard divide and conquer
2. Parallel Merge sort - using ForkJoinPool framework

Both the algorithms are run on a list of 10^8 integers, and their runtimes are as follows :-

| Number of integers      | Sequential Merge Sort      | Parallel Merge Sort             |
| ------------------      | -------------------------- | ------------------------------- |
| 10^8                    | 100188 millis (100 seconds)| 46251 millis (46 seconds)       |


`Parallel Merge Sort` takes around half the time of `Sequential Merge Sort`.
