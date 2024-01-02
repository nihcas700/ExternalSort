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

public class MergeSortedAndFlushKway implements Runnable {
    private List<BufferedReader> readers;
    private BufferedWriter writer;
    private int bufferSize;
    private ThreadMetadata metadata;

    public MergeSortedAndFlushKway(List<BufferedReader> readers, BufferedWriter writer, int bufferSize, ThreadMetadata metadata) {
        this.readers = readers;
        this.writer = writer;
        this.bufferSize = bufferSize;
        this.metadata = metadata;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        int totalLines = 0;
        try {
            Iterator<Integer> mergedIterator = Utils.merge(getIterators(readers));
            // Flush it out to disk
            while (mergedIterator.hasNext()) {
                totalLines++;
                writer.write(mergedIterator.next() + "\n");
            }
            List<Closeable> closeables = new ArrayList<>(readers);
            closeables.add(writer);
            closeCloseables(closeables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        metadata.setRunTime(end - start);
        metadata.setLinesProcessed(totalLines);
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
