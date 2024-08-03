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
import java.util.concurrent.Executor;

import static utils.FileUtility.*;

public class GatherAndMergeJob {
    private int inputChunkSize;
    private int outputBufferSize;
    private Executor executor;
    private static final Logger LOGGER = LogManager.getLogger(GatherAndMergeJob.class);
    private Logger log = LOGGER;

    public GatherAndMergeJob(final int inputChunkSize, final int outputBufferSize, final Executor executor) {
        this.inputChunkSize = inputChunkSize;
        this.outputBufferSize = outputBufferSize;
        this.executor = executor;
    }

    public void gatherAndMerge(final String intermediateFilePath, final String finalOutputPath, int K) throws Exception {
        long start = System.currentTimeMillis();
        int layer = 0;
        while (doLayerXFilesExist(intermediateFilePath, layer)) {
            Map<String, ThreadMetadata> threadMetadata = new LinkedHashMap<>();
            List<File> filesX = new ArrayList<>(getLayerXFiles(intermediateFilePath, layer).values());
            log.info("Found " + filesX.size() + " files in layer " + layer);
            if (filesX.size() == 1) {
                filesX.get(0).renameTo(new File(finalOutputPath));
                break;
            }
            int fileNo = 1;
            for (int i = 0; i < filesX.size(); i += K) {
                List<BufferedReader> readers = new ArrayList<>();
                for (int j = 0; j < K; j++) {
                    if ((i + j) >= filesX.size()) {
                        break;
                    }
                    readers.add(new BufferedReader(new FileReader(filesX.get(i + j)), inputChunkSize));
                }
                String outputFileName = getIntermediateFileName(fileNo, layer + 1);
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath, outputFileName).toString()), outputBufferSize);
                ThreadMetadata metadata = new ThreadMetadata();
                metadata.setFileName(outputFileName);
                threadMetadata.put(outputFileName, metadata);
                metadata.setFuture(getMergeSortedFuture(readers, writer, metadata, executor));
                fileNo++;
            }
            Utils.waitForThreadsToComplete(threadMetadata, layer, log);
            layer++;
        }
        long end = System.currentTimeMillis();
        log.info("[mergeAndSort] Merge and sort took " + (end - start) + " millis");
    }

    public CompletableFuture<Object> getMergeSortedFuture(List<BufferedReader> readers, BufferedWriter writer,
                                                          ThreadMetadata metadata, Executor executor) {
        return CompletableFuture.runAsync(new MergeSortedAndFlushKway(readers, writer, metadata), executor)
                .exceptionally((ex) -> Utils.printException(metadata, ex, log))
                .thenApply((result) -> Utils.printThreadMetadataDetails(metadata, log));
    }
}
