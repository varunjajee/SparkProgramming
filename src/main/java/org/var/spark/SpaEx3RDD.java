package org.var.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SpaEx3RDD {
    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("Counter")
                        .set("spark.worker.cleanup.enabled", "true");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String inputFilePath = "/Users/varunjajee/Data/spark/javaProgrammingGit/SparkProgramming/src/main/resources/word_count.txt";
        int iPartitions = 3;
        JavaRDD<String> inputFileRDDs;
        inputFileRDDs = sparkContext.textFile(inputFilePath, iPartitions);
        long count = inputFileRDDs.count();
        System.out.println("\n\n\n No of lines in file " + count);
    }


}
