package inmemory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Disabled
public class TestSortingAlgorithms {

    static int size;
    static BufferedWriter writer;

    static final String OUTPUT_FILE_PATH = "./src/test/java/inmemory/TestSortResult.txt";

    @BeforeAll
    public static void setup() throws IOException {
        size = 100000000;
        File outputFile = new File(OUTPUT_FILE_PATH);
        outputFile.createNewFile();
        writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH));
        writer.write("List Size:" + size + "\n");
    }

    @AfterAll
    public static void tearDown() throws IOException {
        writer.flush();
        writer.close();
    }

    @Test
    @Disabled
    public void runSequentialQS() throws IOException {
        writer.write("Running Sequential Quick Sort" + "\n");
        long start = System.currentTimeMillis();
        if (runTest(size, new SequentialQuickSort())) {
            writer.write("Test Passed" + "\n");
        } else {
            writer.write("Test Failed!!!!" + "\n");
        }
        long end = System.currentTimeMillis();
        writer.write("The test took " + (end - start) + " millis" + "\n");
        writer.write("=================================" + "\n");
        writer.flush();
    }

    @Test
    public void runParallelMS() throws IOException {
        writer.write("Running Parallel Merge Sort" + "\n");
        long start = System.currentTimeMillis();
        if (runTest(size, new ParallelMergeSort())) {
            writer.write("Test Passed" + "\n");
        } else {
            writer.write("Test Failed!!!!" + "\n");
        }
        long end = System.currentTimeMillis();
        writer.write("The test took " + (end - start) + " millis" + "\n");
        writer.write("=================================" + "\n");
        writer.flush();
    }

    @Test
    public void runSequentialMS() throws IOException {
        writer.write("Running Sequential Merge Sort" + "\n");
        long start = System.currentTimeMillis();
        if (runTest(size, new SequentialMergeSort())) {
            writer.write("Test Passed" + "\n");
        } else {
            writer.write("Test Failed!!!!" + "\n");
        }
        long end = System.currentTimeMillis();
        writer.write("The test took " + (end - start) + " millis" + "\n");
        writer.write("=================================" + "\n");
        writer.flush();
    }

    private static boolean runTest(int size, SortingAlgorithm sort) {
        // Initialize
        List<Integer> list = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            list.add(random.nextInt(1000));
        }

        // Sort
        sort.sort(list);

        // Test
        for (int i = 0; i < size; i++) {
            if (i > 0 && list.get(i) < list.get(i-1)) {
                return false;
            }
        }
        return true;
    }
}
