import diskbased.ExternalSort;
import utils.Constants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
            if (argList.contains("createInput")) {
                createInput();
            }
            if (argList.contains("clearIntermediate")) {
                clearIntermediate();
            }
        }
        long start = System.currentTimeMillis();
        ExternalSort externalSort = new ExternalSort(INPUT_CHUNK_SIZE, OUTPUT_BUFFER_SIZE, OUTPUT_CHUNK_SIZE, SEQUENTIAL_QS);
        externalSort.sort(INTERMEDIATE_PATH_STR, INPUT_FILE_PATH_STR, OUTPUT_FILE_PATH_STR);
        long end = System.currentTimeMillis();
        System.out.println("The test took " + (end - start) + " millis" + "\n");
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

    private static void createInput() throws IOException {
        new File(WORKING_DIR).mkdir();
        new File(INTERMEDIATE_PATH_STR).mkdir();
        new File(Constants.INPUT_FILE_PATH_STR).createNewFile();
        new File(Constants.OUTPUT_FILE_PATH_STR).createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(Constants.INPUT_FILE_PATH_STR), 1000000);
        long size = 1000000000L;
        Random random = new Random();
        long start = System.currentTimeMillis();
        for (long i = 0; i < size; i++) {
            writer.write(random.nextInt(1000000) + "\n");
        }
        writer.close();
        long end = System.currentTimeMillis();
        System.out.println("Input creation took " + (end - start) + " millis" + "\n");
    }
}
