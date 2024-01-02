package diskbased.blockingio;

import diskbased.ThreadMetadata;
import utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeSortedAndFlushKway extends MonitoredRunnable {
    private List<BufferedReader> readers;
    private BufferedWriter writer;

    public MergeSortedAndFlushKway(List<BufferedReader> readers, BufferedWriter writer,
                                   ThreadMetadata metadata) {
        super(metadata);
        this.readers = readers;
        this.writer = writer;
    }

    @Override
    public void doRun() {
        try {
            Iterator<Integer> mergedIterator = Utils.merge(getIterators(readers));
            // Flush it out to disk
            while (mergedIterator.hasNext()) {
                incrementLinesProcessed();
                writer.write(mergedIterator.next() + "\n");
            }
            List<Closeable> closeables = new ArrayList<>(readers);
            closeables.add(writer);
            closeCloseables(closeables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeCloseables(List<Closeable> closeables) throws IOException {
        for (Closeable closeable : closeables) {
            if (closeable != null) {
                closeable.close();
            }
        }
    }

    private List<Iterator<Integer>> getIterators(List<BufferedReader> readers) {
        List<Iterator<Integer>> list = new ArrayList<>();
        for (BufferedReader reader : readers) {
            list.add(Utils.iteratorFromBuffReader(reader));
        }
        return list;
    }
}
