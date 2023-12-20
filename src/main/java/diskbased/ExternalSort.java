package diskbased;

import diskbased.runnables.MergeSortedAndFlush;
import diskbased.runnables.SortAndFlush;
import inmemory.ParallelMergeSort;
import inmemory.SequentialMergeSort;
import inmemory.SequentialQuickSort;
import inmemory.SortingAlgorithm;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static utils.Constants.PARALLEL_MS;
import static utils.Constants.SEQUENTIAL_QS;
import static utils.FileUtility.*;

public class ExternalSort {
    private int inputChunkSize;
    private int outputBufferSize;
    private int outputChunkSize;
    private String mergeSortImpl;
    public ExternalSort(int inputChunkSize, int outputBufferSize, int outputChunkSize, String mergeSortImpl) {
        this.inputChunkSize = inputChunkSize;
        this.outputBufferSize = outputBufferSize;
        this.outputChunkSize = outputChunkSize;
        this.mergeSortImpl = mergeSortImpl;
    }
    public void sort(final String intermediateFilePath, String inputFilePath, String outputPath) throws Exception {
        divideAndSort(intermediateFilePath, inputFilePath);
        mergeAndSort(intermediateFilePath, outputPath);
    }

    private void mergeAndSort(final String intermediateFilePath, final String finalOutputPath) throws Exception {
        long start = System.currentTimeMillis();
        int layer = 0;
        while (doLayerXFilesExist(intermediateFilePath, layer)) {
            Map<String, ThreadMetadata> threadMetadata = new LinkedHashMap<>();
            List<File> filesX = new ArrayList<>(getLayerXFiles(intermediateFilePath, layer).values());
            System.out.println("Found " + filesX.size() + " files in layer " + layer);
            if (filesX.size() == 1) {
                filesX.get(0).renameTo(new File(finalOutputPath));
                break;
            }
            int fileNo = 1;
            for (int i = 0; i < filesX.size(); i+=2) {
                BufferedReader firstReader = new BufferedReader(new FileReader(filesX.get(i)));
                String outputFileName = getIntermediateFileName(fileNo, layer+1);
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath,
                        outputFileName).toString()), outputBufferSize);
                BufferedReader secondReader = null;
                if (i+1 < filesX.size()) {
                    secondReader = new BufferedReader(new FileReader(filesX.get(i+1)));
                }
                ThreadMetadata metadata = new ThreadMetadata();
                metadata.setFileName(outputFileName);
                threadMetadata.put(outputFileName, metadata);
                metadata.setFuture(getMergeSortedFuture(firstReader, secondReader, writer, outputChunkSize, metadata));
                fileNo++;
            }
            waitForThreadsToComplete(threadMetadata, layer);
            layer++;
        }
        long end = System.currentTimeMillis();
        System.out.println("[mergeAndSort] Merge and sort took " + (end - start) + " millis");
    }

    private static void waitForThreadsToComplete(Map<String, ThreadMetadata> threadMetadata, final int layer) {
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
            System.out.println("Execution of layer " + layer + " done. " +
                    "Total Lines=" + totalLines + ". " +
                    "Total CPU=" + totalCPU + ". " +
                    "Total IO=" + totalIO + ". " +
                    "Total IORead=" + totalReadIO + ". " +
                    "Total IOWrite=" + totalWriteIO);
            return null;
        }).join();
    }

    private static CompletableFuture<Object> getMergeSortedFuture(BufferedReader firstReader, BufferedReader secondReader,
                                                                  BufferedWriter writer, int bufferSize,
                                                                  ThreadMetadata metadata) {
        return CompletableFuture
                .runAsync(new MergeSortedAndFlush(firstReader, secondReader, writer, bufferSize, metadata))
                .exceptionally((ex) -> printException(metadata, ex))
                .thenApply((result) -> printThreadMetadataDetails(metadata));
    }
    private static Void printThreadMetadataDetails(ThreadMetadata metadata) {
        System.out.println("Thread " + metadata.getFileName() + " took " +
                metadata.getRunTime() + " millis");
        return null;
    }

    private static Void printException(ThreadMetadata metadata, Throwable ex) {
        System.out.println("Exception Occurred in " + metadata.getFileName());
        ex.printStackTrace();
        return null;
    }

    private void divideAndSort(String intermediateFilePath, String inputFilePath) throws IOException {
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
        String line;
        int counter = 1, fileNo = 1;
        List<Integer> list = new ArrayList<>();
        Map<String, ThreadMetadata> threadMetadata = new LinkedHashMap<>();
        while ((line= reader.readLine()) != null) {
            if (counter % inputChunkSize == 0) {
                String fileName = getIntermediateFileName(fileNo, 0);
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath, fileName).toString()), outputBufferSize);

                // Populate thread metadata
                ThreadMetadata metadata = new ThreadMetadata();
                metadata.setFileName(fileName);
                threadMetadata.put(fileName, metadata);
                metadata.setFuture(getSortAndFlushFuture(writer, list, metadata));
                list.clear();
                fileNo++;
            }
            list.add(Integer.parseInt(line));
            counter++;
        }
        waitForThreadsToComplete(threadMetadata, 0);
        long end = System.currentTimeMillis();
        System.out.println("[divideAndSort] Divide and sort took " + (end - start) + " millis");
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
