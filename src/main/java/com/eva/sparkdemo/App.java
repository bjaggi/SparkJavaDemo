package com.eva.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.sql.SQLContext;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);

       // sqlContext.js
        JavaRDD<String> spam = jsc.textFile("spam.txt");
        JavaRDD<String> normal = jsc.textFile("normal.txt");

        //HashingTF ht = new HashingTF(10000);

        		
    }
}
