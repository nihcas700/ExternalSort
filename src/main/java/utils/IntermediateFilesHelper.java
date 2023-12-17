package utils;

import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class IntermediateFilesHelper {
    private static final String FILE_PREFIX = "file";
    private static final String DELIMITER = "-";

    public static String getIntermediateFileName(int counter, int layerX) {
        String fileName = String.join(DELIMITER, FILE_PREFIX, String.valueOf(counter), String.valueOf(layerX));
        return fileName + DELIMITER + "output.txt";
    }

    public static boolean doLayerXFilesExist(String intermediateFilePath, int layer) {
        return getAllIntermediateFiles(intermediateFilePath).keySet()
                .stream()
                .anyMatch(f -> (getLayerFromFileName(f) == layer));
    }

    private static int getLayerFromFileName(String fileName) {
        String[] tuples = fileName.split(DELIMITER);
        if (tuples.length < 4) {
            throw new RuntimeException("Invalid file name:" + fileName);
        }
        return Integer.parseInt(tuples[2]);
    }

    public static Map<String, File> getLayerXFiles(String intermediateFilePath, final int layer) {
        Map<String, File> files = getAllIntermediateFiles(intermediateFilePath);
        return files.keySet().stream()
                .filter(f -> (getLayerFromFileName(f) == layer))
                .collect(Collectors.toMap(identity(), files::get));
    }

    public static Map<String, File> getAllIntermediateFiles(String intermediateFilePath) {
        return Stream
                .of(Objects.requireNonNull(new File(intermediateFilePath).listFiles()))
                .collect(Collectors.toMap(File::getName, identity()));
    }
}

