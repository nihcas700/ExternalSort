package diskbased;

public interface ExternalSort {
    void sort(final String intermediateFilePath, String inputFilePath, String outputPath) throws Exception;
}
