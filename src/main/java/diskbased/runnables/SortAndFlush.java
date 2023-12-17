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
        // Sort the list
        long start = System.currentTimeMillis();
        sortImpl.sort(list);
        long firstEnd = System.currentTimeMillis();

        // Write to disk
        int linesProcessed = 0;
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
        long secondEnd = System.currentTimeMillis();

        // Set metadata
        metadata.setRunTime(secondEnd-start);
        metadata.setCpuTime(firstEnd-start);
        metadata.setIoTime(secondEnd-firstEnd);
        metadata.setIoReadTime(0);
        metadata.setIoWriteTime(metadata.getIoTime() - metadata.getIoReadTime());
        metadata.setLinesProcessed(linesProcessed);
    }
}
