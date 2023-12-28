package diskbased;

public interface ExternalSort {
    void sort(final String intermediateFilePath, String inputFilePath, String outputPath, int K) throws Exception;
}
