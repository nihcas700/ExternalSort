package utils;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static void merge(List<Integer> list, int start, int mid, int end) {
        if (start > mid || mid+1 > end) {
            return;
        }
        List<Integer> tempList = new ArrayList<>();
        for (int i = mid+1; i <= end; i++) {
            tempList.add(list.get(i));
        }
        int firstListIndex = mid, secondListIndex =  (tempList.size() - 1), finalListIndex = end;
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

    public static List<Integer> merge(List<Integer> firstList,
                             List<Integer> secondList) {
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
}
