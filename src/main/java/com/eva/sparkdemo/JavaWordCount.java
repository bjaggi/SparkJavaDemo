package com.eva.sparkdemo; /**
 * Created by jaggib2 on 11/3/2016.
 */

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Softwares\\hadoop-common-2.2.0-bin-master");

        if (args.length < 1) {
            //System.err.println("Usage: com.eva.sparkdemo.JavaWordCount <file>");
            //System.exit(1);
        }
        
        

   /*     SparkSession spark = SparkSession
                .builder()
                .appName("com.eva.sparkdemo.JavaWordCount").master("https://localhost:8080")
                .getOrCreate();*/
        
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        
        

        //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> lines = jsc.textFile("C:\\workspace\\trump_web_crawler.txt");//args[0]);
        
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            //@Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    //@Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    //@Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        jsc.stop();
    }
}
