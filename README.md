This project contains the implementations of in-memory and diskbased sorting algorithms and compares their runtimes across various optimizations. The code was run on a machine with 8-core CPU with 32 GB RAM.

# In-memory Merge sort

[This package](https://github.com/nihcas700/ExternalSort/tree/master/src/main/java/inmemory) contains the following implementations

1. Sequential Merge sort - using standard divide and conquer
2. Parallel Merge sort - using ForkJoinPool framework

Both the algorithms are run on a list of 10^8 integers, and their runtimes are as follows :-

```

| Number of integers      | Sequential Merge Sort      | Parallel Merge Sort             |
| ------------------      | -------------------------- | ------------------------------- |
| 10^8                    | 100188 millis (100 seconds)| 46251 millis (46 seconds)       |

```

As can be seen, `Parallel Merge Sort` takes around half the time of `Sequential Merge Sort`.

# External Sort

[This package](https://github.com/nihcas700/ExternalSort/tree/master/src/main/java/diskbased) contains the following implementations

1. External Sort with blocking IO
2. External Sort with async blocking IO (Only reads are async)

The algorithms are run on an input file containing 10^9 integers, and their runtimes are as follows :-

```

| Number of integers | External Sort with blocking IO | External Sort with async blocking IO   |
| ------------------ | ------------------------------ | -------------------------------------- |
| 10^9               | 1225.730 seconds (20.42 mins)  | 1160.744 seconds (19.34 mins)          |

```

The following sections document the optimizations that were made to both the implementations 
and their performance implications along the way.

# Optimization 1 - Changed the way two sorted lists were merged.
Initially, the signature of the merge method looked like `merge(list, start, mid, end)`. It expected
a single list containing two sorted sub-lists - first ranging from start to mid and the next sublist ranging from
mid+1 to end. The implementation was copying the second sublist (mid+1 to end) over to a new list
before performing a merge, eventually having the final sorted list in the input list itself.

The optimization here was to avoid the copying by providing two sorted lists in the input itself. Now, the
method signature looks like `merge(list1, list2)`. Their updated runtimes are as follows :- 

| Number of integers | External Sort with blocking IO | External Sort with async blocking IO |
| ------------------ |--------------------------------|--------------------------------------|
| 10^9               | 929.743 seconds (15.49 mins)   | 936.607 seconds (15.61 mins)         |
