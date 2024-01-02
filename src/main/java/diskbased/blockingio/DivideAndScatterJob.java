package diskbased.blockingio;

import diskbased.ThreadMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.Utils;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static utils.FileUtility.getIntermediateFileName;

public class DivideAndScatterJob {
    private int inputChunkSize;
    private String sortImpl;
    private int outputBufferSize;
    private static final Logger LOGGER = LogManager.getLogger(DivideAndScatterJob.class);
    private Logger log = LOGGER;

    public DivideAndScatterJob(final int inputChunkSize, String sortImpl, final int outputBufferSize) {
        this.inputChunkSize = inputChunkSize;
        this.sortImpl = sortImpl;
        this.outputBufferSize = outputBufferSize;
    }

    public void divideAndScatter(String intermediateFilePath, String inputFilePath) throws IOException {
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath), inputChunkSize);

        int counter = 1, fileNo = 1;
        List<Integer> list = new ArrayList<>(inputChunkSize);
        Map<String, ThreadMetadata> threadMetadata = new LinkedHashMap<>();
        while (true) {
            String line = reader.readLine();
            if (counter % inputChunkSize == 0 || line == null) {
                String fileName = getIntermediateFileName(fileNo, 0);
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath, fileName).toString()), outputBufferSize);

                // Populate thread metadata
                ThreadMetadata metadata = new ThreadMetadata();
                metadata.setFileName(fileName);
                threadMetadata.put(fileName, metadata);
                metadata.setFuture(getSortAndFlushFuture(writer, list, metadata));
                list = new ArrayList<>(inputChunkSize + 1);
                fileNo++;
                if (line == null) break;
            }
            list.add(Integer.parseInt(line));
            counter++;
        }
        Utils.waitForThreadsToComplete(threadMetadata, -1, log);
        long end = System.currentTimeMillis();
        log.info("[divideAndSort] Divide and sort took " + (end - start) + " millis");
    }

    private CompletableFuture<Object> getSortAndFlushFuture(BufferedWriter writer, List<Integer> list, ThreadMetadata metadata) {
        SortAndFlush sortAndFlush = new SortAndFlush(writer, list, Utils.getSortingAlgorithm(sortImpl), metadata);
        return CompletableFuture.runAsync(sortAndFlush)
                .exceptionally((ex) -> Utils.printException(metadata, ex, log))
                .thenApply((result) -> Utils.printThreadMetadataDetails(metadata, log));
    }
}
