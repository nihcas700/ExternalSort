package diskbased.asyncio;

import diskbased.ThreadMetadata;
import utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeSortedAndFlush implements Runnable {
    private FileIOPubSub firstPubSub;
    private FileIOPubSub secondPubSub;
    private BufferedWriter writer;
    private int bufferSize;
    private ThreadMetadata metadata;

    public MergeSortedAndFlush(String firstFile, String secondFile,
                               BufferedWriter writer, int bufferSize,
                               ThreadMetadata metadata) throws IOException {
        this.firstPubSub = new FileIOPubSub(bufferSize, 5*bufferSize, firstFile);
        this.secondPubSub = new FileIOPubSub(bufferSize, 5*bufferSize, secondFile);
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
                List<Integer> firstList = new ArrayList<>(readFile(firstPubSub, bufferSize, metadata));
                List<Integer> secondList = new ArrayList<>(readFile(secondPubSub, bufferSize, metadata));
                if (firstList.isEmpty() && secondList.isEmpty()) break;

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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
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

    private List<Integer> readFile(FileIOPubSub fileIOPubSub, int bufferSize, ThreadMetadata metadata) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        List<Integer> list = fileIOPubSub.readInts(bufferSize);
        long end = System.currentTimeMillis();
        metadata.setIoReadTime((end-start) + metadata.getIoReadTime());
        return list;
    }
}
