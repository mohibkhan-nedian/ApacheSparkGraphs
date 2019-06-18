/**
 * Created by Mohib on 3/25/2019.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.lambda3.Graphene;

public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        /*if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
*/
        Graphene graphene = new Graphene();

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        /*SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();*/
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");// run in local mode
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("wordcount.txt", 2);  //splits file into 2 The textFile method also takes an optional second argument for controlling the number of partitions of the file. By default, Spark creates one partition for each block of the file (blocks being 128MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value. Note that you cannot have fewer partitions than blocks.

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator()); //for a line we get words from it

        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1)); // word and 1 is a tuple, a tuple is 2 items together like a pair

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((i1, i2) -> i1 + i2); // add counts from the left and the right. if we have like 3 records like cat 5, cat 3, cat 2
        // reducebykey will add two by two, so first add cat5 with cat 3 then with cat 2 or in any other order it doesnt matter cause we are adding.
        // the good things is that it just takes two over time so no matter even if you add millions it will take two over time

        List<Tuple2<String, Integer>> output = counts.collect();    //utput is a local collection
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        //spark.stop();
    }
}

