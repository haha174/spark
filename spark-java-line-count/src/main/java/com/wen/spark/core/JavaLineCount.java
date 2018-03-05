package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class JavaLineCount {
    public static void main(String[] args){
        SparkConf cf=new SparkConf();
        cf.setMaster("local").setAppName("LineCount");
        JavaSparkContext sc=new JavaSparkContext(cf);
        JavaRDD<String> lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\world-count.txt");
        JavaPairRDD<String,Integer>line=lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairRDD<String,Integer> lineCount=line.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        lineCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"    "+stringIntegerTuple2._2);
            }
        });
    }
}
