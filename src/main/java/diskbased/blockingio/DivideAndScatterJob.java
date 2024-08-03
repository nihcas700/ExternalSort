package diskbased.blockingio;

import diskbased.ThreadMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static utils.FileUtility.getIntermediateFileName;

public class DivideAndScatterJob {
    private int inputChunkSize;
    private String sortImpl;
    private int outputBufferSize;
    private Executor executor;
    private static final Logger LOGGER = LogManager.getLogger(DivideAndScatterJob.class);
    private Logger log = LOGGER;

    public DivideAndScatterJob(final int inputChunkSize, String sortImpl, final int outputBufferSize, final Executor executor) {
        this.inputChunkSize = inputChunkSize;
        this.sortImpl = sortImpl;
        this.outputBufferSize = outputBufferSize;
        this.executor = executor;
    }

    public void divideAndScatter(String intermediateFilePath, String inputFilePath) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile file = new RandomAccessFile(inputFilePath, "rw");
        FileChannel channel = file.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        long pos = 0;
        int fileNo = 1, currNo = 0;
        List<Integer> list = new ArrayList<>();
        Map<String, ThreadMetadata> threadMetadata = new LinkedHashMap<>();
        while (true) {
            int bytesRead = channel.read(buffer, pos);
            buffer.flip();
            currNo = Utils.convertToInt(buffer, currNo, list);
            pos += bytesRead;
            buffer.clear();
            if ((!list.isEmpty() && (list.size() > inputChunkSize)) || bytesRead <= 0) {
                String fileName = getIntermediateFileName(fileNo, 0);
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath, fileName).toString()), outputBufferSize);

                // Populate thread metadata
                ThreadMetadata metadata = new ThreadMetadata();
                metadata.setFileName(fileName);
                threadMetadata.put(fileName, metadata);
                metadata.setFuture(getSortAndFlushFuture(writer, list, metadata));
                list = new ArrayList<>(inputChunkSize + 1);
                fileNo++;
                if (bytesRead <= 0) break;
            }
        }
        Utils.waitForThreadsToComplete(threadMetadata, -1, log);
        long end = System.currentTimeMillis();
        log.info("[divideAndSort] Divide and sort took " + (end - start) + " millis");
    }


    private CompletableFuture<Object> getSortAndFlushFuture(BufferedWriter writer, List<Integer> list, ThreadMetadata metadata) {
        SortAndFlush sortAndFlush = new SortAndFlush(writer, list, Utils.getSortingAlgorithm(sortImpl), metadata);
        return CompletableFuture.runAsync(sortAndFlush, executor)
                .exceptionally((ex) -> Utils.printException(metadata, ex, log))
                .thenApply((result) -> Utils.printThreadMetadataDetails(metadata, log));
    }
}
