package diskbased.runnables;

import diskbased.ThreadMetadata;
import inmemory.SortingAlgorithm;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class SortAndFlush implements Runnable {
    private BufferedWriter writer;
    private List<Integer> list;
    private SortingAlgorithm sortImpl;

    private ThreadMetadata metadata;
    public SortAndFlush(BufferedWriter writer, List<Integer> list, SortingAlgorithm sortImpl, ThreadMetadata metadata) {
        this.writer = writer;
        this.list = new ArrayList<>(list);
        this.sortImpl = sortImpl;
        this.metadata = metadata;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        int linesProcessed = 0;
        sortImpl.sort(list);
        for (Integer num : list) {
            try {
                writer.write(num + "\n");
                linesProcessed++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        metadata.setRunTime(end-start);
        metadata.setLinesProcessed(linesProcessed);
    }
}
