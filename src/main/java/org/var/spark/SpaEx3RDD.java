
package org.var.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


/*
basic example of RDDs and pair RDDS

1. MAP
2. Flat MAP
3. Mapto Pair.
4. Reducebykey

* */


public class SpaEx3RDD {
    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("Counter")
                        .set("spark.worker.cleanup.enabled", "true");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String inputFilePath = "/Users/varunjajee/Data/spark/javaProgrammingGit/SparkProgramming/src/main/resources/emp.csv";
        int iPartitions = 3;
        JavaRDD<String> inputFileRDDs;
        inputFileRDDs = sparkContext.textFile(inputFilePath, iPartitions);
        long count = inputFileRDDs.count();
        System.out.println("\n\n\n No of lines in file " + count);

        // Following line prints contects of all RDDS.
        inputFileRDDs.foreach(e->System.out.println(e));

        IntMapRDDTest(sparkContext);

        FlatMap(inputFileRDDs);

        PairRDD(inputFileRDDs);

        // UNION
        JOINRDD(sparkContext);

        //Replace or Filter
        ReplaceRDDContent(sparkContext);

        //
        // ACTIONS
        //
        System.out.println("Action Count");
        System.out.println(inputFileRDDs.count());
        //
        //inputFileRDDs = inputFileRDDs.flatMap(x -> Arrays.asList(x.split(","))).filter(value->value=="7698");
        //JavaRDD<String> newFlatRDD = inputFileRDDs.flatMap(s -> Arrays.asList(s.split(",")).iterator());
    }

    private static void IntMapRDDTest(JavaSparkContext sparkContext)
    {
        JavaRDD<Integer> intRDD = sparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10), 5);
        JavaRDD<Integer> NewIntRDD = intRDD.map(x->x*10);

        System.out.println("\n**********************MAP*************************\n");
        intRDD.foreach(x -> System.out.println("map intRDD" +x));
        NewIntRDD.foreach(x -> System.out.println("map NewIntRDD" +x));
    }

    private static void FlatMap(JavaRDD<String> inputFileRDDs)
    {
        JavaRDD<String> newFlatRDD = inputFileRDDs.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        newFlatRDD.foreach(x -> System.out.println("newFlatRDD" +x));
    }

    private static void PairRDD(JavaRDD<String> inputFileRDDs)
    {
        JavaRDD<String> newFlatRDD = inputFileRDDs.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        JavaPairRDD<String,Integer> mapToPairRDD = newFlatRDD.mapToPair(word -> new Tuple2<>(word,1));
        mapToPairRDD.foreach(data -> System.out.println("Key= "+data._1() + "Value="+data._2()));
        JavaPairRDD<String,Integer> reduceByKeyRDD = mapToPairRDD.reduceByKey((a,b)->a+b);
        reduceByKeyRDD.foreach(data -> System.out.println("Key2= "+data._1() + " Value2= "+data._2()));

    }
    // UNION
    private static void JOINRDD(JavaSparkContext sparkContext)
    {
        JavaRDD<Integer> intRDD1 = sparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        JavaRDD<Integer> intRDD2 = sparkContext.parallelize(Arrays.asList(11,12,13,14));

        JavaRDD<Integer> res = intRDD1.union(intRDD2);

        res.foreach(x -> System.out.println("test Join RDDs "+ x));
    }

    private static void ReplaceRDDContent(JavaSparkContext sparkContext)
    {
        JavaRDD<Integer> intRDD1 = sparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));

        //JavaRDD<Integer> newRDD = intRDD1.map(x -> {if(x == 4 || x == 5){ x = 0;} } );

        JavaRDD<Integer> filterRDD1 = intRDD1.filter(x -> x > 4);
        // Filter
        filterRDD1.foreach(x->System.out.println(" test Filter RDD" + x ));

        JavaRDD<Integer> replaceRDD = intRDD1.map(x -> {if (x>=5){ return 0;}else { return 1;}});

        replaceRDD.foreach(x -> System.out.println(" Replace RDD " + x ));

    }
}

