package utils;

import diskbased.ThreadMetadata;
import inmemory.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static utils.Constants.*;

public class Utils {
    public static void merge(List<Integer> list, int start, int mid, int end) {
        if (start > mid || mid + 1 > end) {
            return;
        }
        List<Integer> tempList = new ArrayList<>();
        for (int i = mid + 1; i <= end; i++) {
            tempList.add(list.get(i));
        }
        int firstListIndex = mid, secondListIndex = (tempList.size() - 1), finalListIndex = end;
        while (firstListIndex >= start && secondListIndex >= 0) {
            if (list.get(firstListIndex) >= tempList.get(secondListIndex)) {
                list.set(finalListIndex, list.get(firstListIndex));
                firstListIndex--;
            } else {
                list.set(finalListIndex, tempList.get(secondListIndex));
                secondListIndex--;
            }
            finalListIndex--;
        }
        while (firstListIndex >= start) {
            list.set(finalListIndex, list.get(firstListIndex));
            firstListIndex--;
            finalListIndex--;
        }
        while (secondListIndex >= 0) {
            list.set(finalListIndex, tempList.get(secondListIndex));
            secondListIndex--;
            finalListIndex--;
        }
    }

    public static List<Integer> merge(List<Integer> firstList, List<Integer> secondList) {
        List<Integer> sortedList = new ArrayList<>(firstList.size() + secondList.size());
        int firstIndex = firstList.size() - 1, secondIndex = secondList.size() - 1;
        while (firstIndex >= 0 && secondIndex >= 0) {
            if (firstList.get(firstIndex) >= secondList.get(secondIndex)) {
                sortedList.add(firstList.get(firstIndex));
                firstList.remove(firstIndex);
                firstIndex--;
            } else {
                sortedList.add(secondList.get(secondIndex));
                secondList.remove(secondIndex);
                secondIndex--;
            }
        }
        while (firstIndex >= 0) {
            sortedList.add(firstList.get(firstIndex));
            firstIndex--;
        }
        while (secondIndex >= 0) {
            sortedList.add(secondList.get(secondIndex));
            secondIndex--;
        }
        return sortedList;
    }

    public static Iterator<Integer> merge(final List<Iterator<Integer>> iterators) {
        return new Iterator<>() {
            private PriorityQueue<Pair<Integer, Iterator<Integer>>> pq = new PriorityQueue<>(Comparator.comparingInt(Pair::getLeft));
            private boolean isInitialized = false;

            @Override
            public boolean hasNext() {
                if (!isInitialized) {
                    initialize();
                    isInitialized = true;
                }
                return !pq.isEmpty();
            }

            private void initialize() {
                for (Iterator<Integer> iter : iterators) {
                    if (iter.hasNext()) {
                        pq.add(new ImmutablePair<>(iter.next(), iter));
                    }
                }
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new RuntimeException("No more elements");
                }
                Pair<Integer, Iterator<Integer>> iterPair = pq.poll();
                int toReturn = iterPair.getLeft();
                if (iterPair.getRight().hasNext()) {
                    pq.add(new ImmutablePair<>(iterPair.getRight().next(), iterPair.getRight()));
                }
                return toReturn;
            }
        };
    }

    public static Iterator<Integer> iteratorFromBuffReader(BufferedReader reader) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return reader.ready();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new RuntimeException("No more available elements");
                }
                try {
                    String line = reader.readLine();
                    return Integer.parseInt(line);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static Void printThreadMetadataDetails(ThreadMetadata metadata, Logger log) {
        log.info("Thread " + metadata.getFileName() + " took " +
                metadata.getRunTime() + " millis");
        return null;
    }

    public static Void printException(ThreadMetadata metadata, Throwable ex, Logger log) {
        log.info("Exception Occurred in " + metadata.getFileName());
        ex.printStackTrace();
        return null;
    }

    public static int convertToInt(ByteBuffer data, int initial,
                                   List<Integer> list) {
        int currNo = initial;
        while (data.hasRemaining()) {
            char ch = (char) (data.get() & 0xFF);
            if (ch == '\n') {
                list.add(currNo);
                currNo = 0;
            } else {
                currNo = (currNo * 10) + (ch - '0');
            }
        }
        return currNo;
    }

    public static void waitForThreadsToComplete(Map<String, ThreadMetadata> threadMetadata, final int layer, Logger log) {
        CompletableFuture.allOf(threadMetadata.values().stream()
                .map(ThreadMetadata::getFuture)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture<?>[0])).thenApply(res -> {
            int totalLines = threadMetadata.values().stream()
                    .mapToInt(ThreadMetadata::getLinesProcessed)
                    .sum();
            long totalRuntime = threadMetadata.values().stream()
                    .mapToLong(ThreadMetadata::getRunTime)
                    .sum();
            log.info("Execution of layer " + layer + " done. " +
                    "Total Lines=" + totalLines + ". " +
                    "Total Runtime=" + totalRuntime);
            return null;
        }).join();
    }

    public static SortingAlgorithm getSortingAlgorithm(String sortImpl) {
        SortingAlgorithm algorithm;
        if (PARALLEL_MS.equals(sortImpl)) {
            algorithm = new ParallelMergeSort();
        } else if (SEQUENTIAL_QS.equals(sortImpl)) {
            algorithm = new SequentialQuickSort();
        } else if (COLLECTIONS_PS.equals(sortImpl)){
            algorithm = new CollectionsSort();
        } else {
            algorithm = new SequentialMergeSort();
        }
        return algorithm;
    }

    public static void main(String[] args) {
        List<Integer> list1 = Arrays.asList(1, 3, 5, 6, 8, 10);
        List<Integer> list2 = Arrays.asList(2, 3, 4, 6);
        List<Integer> list3 = Arrays.asList(2, 7, 8, 9);
        System.out.println(merge(Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator())));
    }
}
