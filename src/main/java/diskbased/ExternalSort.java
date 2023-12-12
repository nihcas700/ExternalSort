package diskbased;

import inmemory.ParallelMergeSort;
import inmemory.SequentialMergeSort;
import inmemory.SequentialQuickSort;
import inmemory.SortingAlgorithm;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static utils.Constants.PARALLEL_MS;
import static utils.Constants.SEQUENTIAL_QS;

public class ExternalSort {
    private int inputChunkSize;
    private int outputBufferSize;
    private String mergeSortImpl;
    public ExternalSort(int inputChunkSize, int outputBufferSize, String mergeSortImpl) {
        this.inputChunkSize = inputChunkSize;
        this.outputBufferSize = outputBufferSize;
        this.mergeSortImpl = mergeSortImpl;
    }
    public void sort(final String intermediateFilePath, String inputFilePath, String outputPath) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
        String line;
        int counter = 1;
        List<Integer> list = new ArrayList<>();
        Map<String, ThreadMetadata> threadMetadata = new LinkedHashMap<>();
        while ((line= reader.readLine()) != null) {
            if (counter % inputChunkSize == 0) {
                String fileName = (counter-inputChunkSize+1) + "-" + (counter) + "-output.txt";
                BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(intermediateFilePath, fileName).toString()), outputBufferSize);

                // Populate thread metadata
                ThreadMetadata metadata = new ThreadMetadata();
                SortingAlgorithm algorithm;
                if (PARALLEL_MS.equals(mergeSortImpl)) {
                    algorithm = new ParallelMergeSort();
                } else if (SEQUENTIAL_QS.equals(mergeSortImpl)) {
                    algorithm = new SequentialQuickSort();
                } else {
                    algorithm = new SequentialMergeSort();
                }
                metadata.setFuture(CompletableFuture
                        .runAsync(new SortAndFlush(writer, list, algorithm, metadata))
                        .exceptionally((ex) -> {
                            System.out.println("Exception Occured in " + metadata.getFileName());
                            ex.printStackTrace();
                            return null;
                        })
                        .thenApply((result) -> {
                            System.out.println("Thread " + metadata.getFileName() + " took " +
                                    metadata.getRunTime() + " millis");
                            return null;
                        }));
                metadata.setStartTime(System.currentTimeMillis());
                metadata.setFileName(fileName);
                threadMetadata.put(fileName, metadata);
                list.clear();
            }
            list.add(Integer.parseInt(line));
            counter++;
        }
        CompletableFuture.allOf(threadMetadata.values().stream()
                .map(ThreadMetadata::getFuture)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture<?>[0])).thenApply(res -> {
            System.out.println("Execution Done.");
            return null;
        }).join();
    }

    public static class ThreadMetadata {
        public CompletableFuture<?> getFuture() {
            return future;
        }

        public void setFuture(CompletableFuture<?> future) {
            this.future = future;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        private String fileName;
        private CompletableFuture<?> future;
        private long startTime;
        private long endTime;

        public long getRunTime() {
            return runTime;
        }

        public void setRunTime(long runTime) {
            this.runTime = runTime;
        }

        private long runTime;
    }

    public static class SortAndFlush implements Runnable {
        private BufferedWriter writer;
        private List<Integer> list;
        private SortingAlgorithm sortImpl;

        private ThreadMetadata metadata;
        public SortAndFlush(BufferedWriter writer, List<Integer> list, SortingAlgorithm sortImpl, ThreadMetadata metadata) {
            this.writer = writer;
            this.list = new ArrayList<>(list);
            this.sortImpl = sortImpl;
            this.metadata = metadata;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            sortImpl.sort(list);
            for (Integer num : list) {
                try {
                    writer.write(num + "\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long end = System.currentTimeMillis();
            metadata.setRunTime(end-start);
        }
    }
}
