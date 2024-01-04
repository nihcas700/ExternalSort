package inmemory;

import java.util.Collections;
import java.util.List;

public class CollectionsSort implements SortingAlgorithm {

    @Override
    public void sort(List<Integer> list) {
        Collections.sort(list);
    }
}
