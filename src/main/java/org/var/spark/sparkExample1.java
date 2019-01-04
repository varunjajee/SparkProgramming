package org.var.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class sparkExample1 {
    public static void main(String[] args) {
        String inputFilePath = "/Users/varunjajee/Data/spark/javaProgrammingGit/SparkProgramming/src/main/resources/word_count.txt";

        SparkConf sparkConf =
                new SparkConf()
                        .setMaster("local")
                        .setAppName("JD Word Counter")
                        .set("spark.worker.cleanup.enabled", "true");

        JavaRDD<String> inputFileRDDs;
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {

            inputFileRDDs = sparkContext.textFile(inputFilePath, 4);
            System.out.println("+ Partitions size:" + inputFileRDDs.partitions().size());


            JavaPairRDD<String, Integer> countData
                    = inputFileRDDs
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b);
            countData.saveAsTextFile("/Users/varunjajee/Data/spark/javaProgrammingGit/SparkProgramming/Example1Output");
            // run "cat part*" from Example1Output folder.
            //Example1Output needs to be deleted every time before running the application.

            // none

        } catch (Exception e) {
            e.printStackTrace();

        } finally {

        }

    }


}
