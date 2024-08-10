package diskbased.spark;

import diskbased.ExternalSort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Function1;
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

//        performSorting(intermediateFilePath, inputFilePath);
        performSortingUsingDF(intermediateFilePath, inputFilePath);

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

    private void performSorting(String intermediateFilePath, String inputFilePath) throws InterruptedException {
        // Initialize SparkConf and JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("External Sort").setMaster("local[*]");
        conf.set("spark.executor.memory", "4g");
        conf.set("spark.driver.memory", "4g");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // Read the input file into an RDD of Strings
        JavaRDD<Integer> integers = sc.textFile(inputFilePath, 32).map(Integer::parseInt);

        integers.sortBy(x -> x, true, integers.getNumPartitions());

        // Sort the integers
        JavaRDD<Integer> sortedIntegers = integers.sortBy(x -> x, true, 1);

        // Convert the sorted integers back to strings
        JavaRDD<String> sortedStrings = sortedIntegers.map(Object::toString);

        // Save the sorted integers to the output file
        sortedStrings.coalesce(1).saveAsTextFile(intermediateFilePath);

        // Stop the SparkContext
//        Thread.sleep(2000000);
        sc.stop();
    }

    private void performSortingUsingDF(String intermediateFilePath, String inputFilePath) throws InterruptedException {
        // Initialize SparkConf and JavaSparkContext
        SparkConf conf = new SparkConf()
                .setAppName("External Sort")
                .setMaster("local[*]");
        conf.set("spark.executor.memory", "4g");
        conf.set("spark.driver.memory", "4g");
        SparkSession spark = SparkSession
                .builder()
                .appName("External Sort")
                .config(conf)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Read the input file into an RDD of Strings
        Dataset<String> df = spark.read().textFile(inputFilePath);
        Dataset<String> stringDataset = df.map(new StrToInt(), Encoders.INT())
                .orderBy("value")
                .map(
                        new IntToStr(),
                        Encoders.STRING()
                );
        stringDataset.coalesce(1)
                .write()
                .text(intermediateFilePath);
//        Thread.sleep(2000000);
        spark.stop();
    }

    static class StrToInt implements Function1<String, Integer>, Serializable {
        @Override
        public Integer apply(String v1) {
            return Integer.parseInt(v1);
        }
    }

    static class IntToStr implements Function1<Integer, String>, Serializable {
        @Override
        public String apply(Integer v1) {
            return v1.toString();
        }
    }
}
