package diskbased.blockingio;

import diskbased.ExternalSort;
import diskbased.ThreadMetadata;
import inmemory.ParallelMergeSort;
import inmemory.SequentialMergeSort;
import inmemory.SequentialQuickSort;
import inmemory.SortingAlgorithm;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static utils.Constants.PARALLEL_MS;
import static utils.Constants.SEQUENTIAL_QS;
import static utils.FileUtility.*;

public class ExternalSortBlockingIO implements ExternalSort {
    private static final Logger LOGGER = LogManager.getLogger(ExternalSortBlockingIO.class);
    private Logger log = LOGGER;
    private int inputChunkSize;
    private int outputBufferSize;
    private int outputChunkSize;
    private String mergeSortImpl;

    public ExternalSortBlockingIO(int inputChunkSize, int outputBufferSize, int outputChunkSize, String mergeSortImpl) {
        this.inputChunkSize = inputChunkSize;
        this.outputBufferSize = outputBufferSize;
        this.outputChunkSize = outputChunkSize;
        this.mergeSortImpl = mergeSortImpl;
    }

    @Override
    public void sort(final String intermediateFilePath, String inputFilePath, String outputPath, int K) throws Exception {
        divideAndScatter(intermediateFilePath, inputFilePath);
        gatherAndMerge(intermediateFilePath, outputPath, K);
    }

    private void gatherAndMerge(final String intermediateFilePath, final String finalOutputPath, int K) throws Exception {
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
            for (int i = 0; i < filesX.size(); i+=K) {
                List<BufferedReader> readers = new ArrayList<>();
                for (int j = 0; j < K; j++) {
                    if ((i+j) >= filesX.size()) {
                        break;
                    }
                    readers.add(new BufferedReader(new FileReader(filesX.get(i+j))));
                }
                String outputFileName = getIntermediateFileName(fileNo, layer+1);
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath,
                        outputFileName).toString()), outputBufferSize);
                ThreadMetadata metadata = new ThreadMetadata();
                metadata.setFileName(outputFileName);
                threadMetadata.put(outputFileName, metadata);
                metadata.setFuture(getMergeSortedFuture(readers, writer, outputChunkSize, metadata));
                fileNo++;
            }
            waitForThreadsToComplete(threadMetadata, layer);
            layer++;
        }
        long end = System.currentTimeMillis();
        log.info("[mergeAndSort] Merge and sort took " + (end - start) + " millis");
    }

    private void waitForThreadsToComplete(Map<String, ThreadMetadata> threadMetadata, final int layer) {
        CompletableFuture.allOf(threadMetadata.values().stream()
                .map(ThreadMetadata::getFuture)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture<?>[0])).thenApply(res -> {
            int totalLines = threadMetadata.values().stream()
                    .mapToInt(ThreadMetadata::getLinesProcessed)
                    .sum();
            long totalCPU = threadMetadata.values().stream()
                    .mapToLong(ThreadMetadata::getCpuTime)
                    .sum();
            long totalIO = threadMetadata.values().stream()
                    .mapToLong(ThreadMetadata::getIoTime)
                    .sum();
            long totalReadIO = threadMetadata.values().stream()
                    .mapToLong(ThreadMetadata::getIoReadTime)
                    .sum();
            long totalWriteIO = threadMetadata.values().stream()
                    .mapToLong(ThreadMetadata::getIoWriteTime)
                    .sum();
            log.info("Execution of layer " + layer + " done. " +
                    "Total Lines=" + totalLines + ". " +
                    "Total CPU=" + totalCPU + ". " +
                    "Total IO=" + totalIO + ". " +
                    "Total IORead=" + totalReadIO + ". " +
                    "Total IOWrite=" + totalWriteIO);
            return null;
        }).join();
    }

    private CompletableFuture<Object> getMergeSortedFuture(List<BufferedReader> readers,
                                                                  BufferedWriter writer, int bufferSize,
                                                                  ThreadMetadata metadata) {
        return CompletableFuture
                .runAsync(new MergeSortedAndFlushKway(readers, writer, bufferSize, metadata))
                .exceptionally((ex) -> printException(metadata, ex))
                .thenApply((result) -> printThreadMetadataDetails(metadata));
    }
    private Void printThreadMetadataDetails(ThreadMetadata metadata) {
        log.info("Thread " + metadata.getFileName() + " took " +
                metadata.getRunTime() + " millis");
        return null;
    }

    private Void printException(ThreadMetadata metadata, Throwable ex) {
        log.info("Exception Occurred in " + metadata.getFileName());
        ex.printStackTrace();
        return null;
    }

    private void divideAndScatter(String intermediateFilePath, String inputFilePath) throws IOException {
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath), inputChunkSize);

        int counter = 1, fileNo = 1;
        List<Integer> list = new ArrayList<>();
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
                list.clear();
                fileNo++;
                if (line == null) break;
            }
            list.add(Integer.parseInt(line));
            counter++;
        }
        waitForThreadsToComplete(threadMetadata, 0);
        long end = System.currentTimeMillis();
        log.info("[divideAndSort] Divide and sort took " + (end - start) + " millis");
    }

    private CompletableFuture<Object> getSortAndFlushFuture(BufferedWriter writer, List<Integer> list, ThreadMetadata metadata) {
        return CompletableFuture
                .runAsync(new SortAndFlush(writer, list, getSortingAlgorithm(mergeSortImpl), metadata))
                .exceptionally((ex) -> printException(metadata, ex))
                .thenApply((result) -> printThreadMetadataDetails(metadata));
    }

    private SortingAlgorithm getSortingAlgorithm(String mergeSortImpl) {
        SortingAlgorithm algorithm;
        if (PARALLEL_MS.equals(mergeSortImpl)) {
            algorithm = new ParallelMergeSort();
        } else if (SEQUENTIAL_QS.equals(mergeSortImpl)) {
            algorithm = new SequentialQuickSort();
        } else {
            algorithm = new SequentialMergeSort();
        }
        return algorithm;
    }
}
