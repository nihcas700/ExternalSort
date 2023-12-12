package inmemory;

import utils.Utils;

import java.util.List;

public class SequentialMergeSort implements SortingAlgorithm {
    @Override
    public void sort(List<Integer> list) {
        mergeSort(list, 0, list.size() - 1);
    }

    private void mergeSort(List<Integer> list, int start, int end) {
        if (start >= end) {
            return;
        }
        int mid = (start + end) / 2;
        mergeSort(list, start, mid);
        mergeSort(list, mid+1, end);
        Utils.merge(list, start, mid, end);
    }
}
