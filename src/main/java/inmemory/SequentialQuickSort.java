package inmemory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class SequentialQuickSort implements SortingAlgorithm {

    private static final Logger LOGGER = LogManager.getLogger(SequentialQuickSort.class);
    private Logger log = LOGGER;
    @Override
    public void sort(List<Integer> list) {
        sort(list, 0, list.size() - 1, new Random());
    }

    private void sort(List<Integer> list, int start, int end, Random random) {
        log.info("Start and End are: [{}-{}]", start, end);
        if (start >= end) {
            return;
        }
        int pivot = (start + random.nextInt(end-start+1));
        int partitionIndex = partition(list, start, pivot, end);
        log.info("Pivot is {}", partitionIndex);
        sort(list, start, partitionIndex-1, random);
        sort(list, partitionIndex+1, end, random);
    }

    private int partition(List<Integer> list, int start, int pivot, int end) {
        if (start == end) {
            return start;
        }
        swap(list, start, pivot);
        int partitionIndex = start;
        for (int i = start+1; i <= end; i++) {
            if (list.get(i) <= list.get(start)) {
                partitionIndex++;
                swap(list, i, partitionIndex);
            }
        }
        swap(list, start, partitionIndex);
        return partitionIndex;
    }

    private void swap(List<Integer> list, int first, int second) {
        if (first != second) {
            int tmp = list.get(first);
            list.set(first, list.get(second));
            list.set(second, tmp);
        }
    }
}
