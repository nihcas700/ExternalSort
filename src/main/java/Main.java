import diskbased.ExternalSort;
import diskbased.blockingio.ExternalSortBlockingIO;
import utils.Constants;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import static utils.Constants.*;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            List<String> argList = List.of(args);
            int[] inputMap = new int[5];
            if (argList.contains("createInput")) {
                inputMap = createInput();
            }
            if (argList.contains("clearIntermediate")) {
                clearIntermediate();
            }
            ExternalSort externalSort = null;
            if (argList.contains("runBlockingIO")) {
                externalSort = new ExternalSortBlockingIO(INPUT_CHUNK_SIZE, OUTPUT_BUFFER_SIZE, SEQUENTIAL_QS);
            } else {
                System.out.println("Unknown External Sort Implementation. Test Failed!!!");
                System.exit(1);
            }
            long start = System.currentTimeMillis();
            externalSort.sort(INTERMEDIATE_PATH_STR, INPUT_FILE_PATH_STR, OUTPUT_FILE_PATH_STR, 33);
            long end = System.currentTimeMillis();
            System.out.println("The test took " + (end - start) + " millis" + "\n");
            if (validateOutput(inputMap)) {
                System.out.println("The test has passed");
            } else {
                System.out.println("The test has failed");
            }
        }
    }

    private static boolean validateOutput(int[] inputMap) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(OUTPUT_FILE_PATH_STR));
        String line;
        int prevNo = -1;
        while ((line = reader.readLine()) != null) {
            int num = Integer.parseInt(line);
            if (num < prevNo) {
                System.out.println("The output is not in the sorted order");
                return false;
            }
            prevNo = num;
            inputMap[num]--;
            if (inputMap[num] < 0) {
                System.out.println("Num:" + num + " has been spotted " +
                        "more than expected");
                return false;
            }
        }
        reader.close();
        int faultyNumsCount = 0;
        for (int i = 0; i < inputMap.length; i++) {
            if (inputMap[i] > 0) {
                faultyNumsCount++;
            }
        }
        System.out.println("Faulty nums count : " + faultyNumsCount);
        return faultyNumsCount == 0;
    }

    private static void clearIntermediate() throws IOException {
        System.out.println("Clearing intermediate directory");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Constants.INTERMEDIATE_PATH)) {
            stream.forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        System.out.println("Done Clearing intermediate directory");
    }

    private static int[] createInput() throws IOException {
        new File(WORKING_DIR).mkdir();
        new File(INTERMEDIATE_PATH_STR).mkdir();
        new File(Constants.INPUT_FILE_PATH_STR).createNewFile();
        new File(Constants.OUTPUT_FILE_PATH_STR).createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(Constants.INPUT_FILE_PATH_STR), 1000000);
        long size = 1000000000L;
        int maxInteger = 1000000;
        int[] map = new int[maxInteger+1];
        Random random = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            int num = random.nextInt(maxInteger);
            map[num]++;
            writer.write(num + "\n");
        }
        writer.close();
        long end = System.currentTimeMillis();
        System.out.println("Input creation took " + (end - start) + " millis" + "\n");
        return map;
    }
}
