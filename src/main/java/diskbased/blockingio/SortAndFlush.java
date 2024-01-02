package diskbased.blockingio;

import diskbased.ThreadMetadata;
import inmemory.SortingAlgorithm;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

public class SortAndFlush extends MonitoredRunnable {
    private BufferedWriter writer;
    private List<Integer> list;
    private SortingAlgorithm sortImpl;

    public SortAndFlush(BufferedWriter writer, List<Integer> list, SortingAlgorithm sortImpl,
                        ThreadMetadata metadata) {
        super(metadata);
        this.writer = writer;
        this.list = new ArrayList<>(list);
        this.sortImpl = sortImpl;
    }

    @Override
    public void doRun() {
        try {
            // Sort the list
            sortImpl.sort(list);

            // Write to disk
            for (Integer num : list) {
                writer.write(num + "\n");
                incrementLinesProcessed();
            }
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
