package diskbased.spark;

import diskbased.ExternalSort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.FileUtility;

import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SparkExternalSort implements ExternalSort, Serializable {
    @Override
    public void sort(String intermediateFilePath, String inputFilePath, String outputPath, int K) throws Exception {
        // Remove intermediate directory
        Files.deleteIfExists(Paths.get(intermediateFilePath));

        // Initialize SparkConf and JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("External Sort").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // Read the input file into an RDD of Strings
        JavaRDD<String> lines = sc.textFile(inputFilePath);

        // Parse the lines to integers
        JavaRDD<Integer> integers = lines.map(Integer::parseInt);

        // Sort the integers
        JavaRDD<Integer> sortedIntegers = integers.sortBy(x -> x, true, 1);

        // Convert the sorted integers back to strings
        JavaRDD<String> sortedStrings = sortedIntegers.map(Object::toString);

        // Save the sorted integers to the output file
        sortedStrings.saveAsTextFile(intermediateFilePath);

        // Stop the SparkContext
        sc.stop();

        // Copy the output to outputPath
        String partFile = FileUtility.getAllIntermediateFiles(intermediateFilePath).entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith("part-"))
                .findFirst()
                .get().getValue().getAbsolutePath();
        RandomAccessFile fromFile = new RandomAccessFile(partFile, "rw");
        FileChannel fromChannel = fromFile.getChannel();
        RandomAccessFile toFile = new RandomAccessFile(outputPath, "rw");
        FileChannel toChannel = toFile.getChannel();
        fromChannel.transferTo(0, fromChannel.size(), toChannel);
    }
}
