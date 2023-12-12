package utils;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static void merge(List<Integer> list, int start, int mid, int end) {
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

}
