package diskbased.blockingio;

import diskbased.ThreadMetadata;
import utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeSortedAndFlush implements Runnable {
    private BufferedReader firstReader;
    private BufferedReader secondReader;
    private BufferedWriter writer;
    private int bufferSize;
    private ThreadMetadata metadata;

    public MergeSortedAndFlush(BufferedReader firstReader, BufferedReader secondReader,
                               BufferedWriter writer, int bufferSize,
                               ThreadMetadata metadata) {
        this.firstReader = firstReader;
        this.secondReader = secondReader;
        this.writer = writer;
        this.bufferSize = bufferSize;
        this.metadata = metadata;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        int totalLines = 0;
        long cputime = 0;
        while (true) {
            try {
                // Read the files
                if ((!isReaderReady(firstReader) && !isReaderReady(secondReader))) break;
                List<Integer> firstList = new ArrayList<>(readFile(firstReader, bufferSize, metadata));
                List<Integer> secondList = new ArrayList<>(readFile(secondReader, bufferSize, metadata));

                // Merge the sorted lists
                long cpuStart = System.currentTimeMillis();
                List<Integer> mergedList = Utils.merge(firstList, secondList);
                long cpuEnd = System.currentTimeMillis();
                cputime += (cpuEnd - cpuStart);

                // Flush it out to disk
                for (int i = mergedList.size()-1; i >= 0; i--) {
                    totalLines++;
                    writer.write(mergedList.get(i) + "\n");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            closeCloseable(firstReader);
            closeCloseable(secondReader);
            closeCloseable(writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        metadata.setRunTime(end - start);
        metadata.setCpuTime(cputime);
        metadata.setIoTime(metadata.getRunTime() - metadata.getCpuTime());
        metadata.setIoWriteTime(metadata.getIoTime() - metadata.getIoReadTime());
        metadata.setLinesProcessed(totalLines);
    }

    private void closeCloseable(Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

    private boolean isReaderReady(BufferedReader reader) throws IOException {
        return reader != null && reader.ready();
    }

    private List<Integer> readFile(BufferedReader reader, int bufferSize, ThreadMetadata metadata) throws IOException {
        long start = System.currentTimeMillis();
        List<Integer> list = new ArrayList<>();
        if (!isReaderReady(reader)) return list;
        int counter = 0;
        String line;
        while (counter < bufferSize && (line = reader.readLine()) != null) {
            list.add(Integer.valueOf(line));
            counter++;
        }
        long end = System.currentTimeMillis();
        metadata.setIoReadTime((end-start) + metadata.getIoReadTime());
        return list;
    }
}
