package diskbased.runnables;

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
        while (true) {
            try {
                if ((!isReaderReady(firstReader) && !isReaderReady(secondReader))) break;
                List<Integer> list = new ArrayList<>();
                list.addAll(readFile(firstReader, bufferSize));
                int first = 0, mid = list.size() - 1;
                list.addAll(readFile(secondReader, bufferSize));
                int last = list.size() - 1;
                Utils.merge(list, first, mid, last);
                for (Integer num : list) {
                    totalLines++;
                    writer.write(num + "\n");
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

    private List<Integer> readFile(BufferedReader reader, int bufferSize) throws IOException {
        List<Integer> list = new ArrayList<>();
        if (!isReaderReady(reader)) return list;
        int counter = 0;
        String line;
        while (counter < bufferSize && (line = reader.readLine()) != null) {
            list.add(Integer.valueOf(line));
            counter++;
        }
        return list;
    }
}
