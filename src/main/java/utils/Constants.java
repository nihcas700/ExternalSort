package utils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Constants {
    public static final String WORKING_DIR = ".";
    public static final String INPUT_FILE_NAME = "input.txt";
    public static final String OUTPUT_FILE_NAME = "output.txt";
    public static final String INTERMEDIATE_DIR = "intermediate";

    // -----------Configuration Start
    public static final int INPUT_CHUNK_SIZE = 1000000;
    public static final int OUTPUT_CHUNK_SIZE = 1000000;
    public static final int OUTPUT_BUFFER_SIZE = 100000;
    public static final String PARALLEL_MS = "PARALLEL_MERGE_SORT";
    public static final String SEQUENTIAL_MS = "SEQUENTIAL_MERGE_SORT";
    public static final String SEQUENTIAL_QS = "SEQUENTIAL_QUICK_SORT";
    public static final String COLLECTIONS_PS = "COLLECTIONS_SORT";
    // -----------Configuration End

    public static final String INPUT_FILE_PATH_STR = Paths.get(WORKING_DIR, INPUT_FILE_NAME).toString();
    public static final String OUTPUT_FILE_PATH_STR = Paths.get(WORKING_DIR, OUTPUT_FILE_NAME).toString();
    public static final String INTERMEDIATE_PATH_STR = Paths.get(WORKING_DIR, INTERMEDIATE_DIR).toString();
    public static final Path INTERMEDIATE_PATH = Paths.get(WORKING_DIR, INTERMEDIATE_DIR);
}
