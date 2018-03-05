package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.List;

import java.util.Arrays;

public class SoftKey {
    public static  void  main(String[] args){
        SparkConf cf=new SparkConf().setMaster("local").setAppName("LineCount");
        JavaSparkContext sc=new JavaSparkContext(cf);
        List<Tuple2<String,Integer>> list= Arrays.asList(new Tuple2<String,Integer>("1",10),new Tuple2<String,Integer>("5",20),new Tuple2<String,Integer>("3",30));
        JavaPairRDD<String,Integer> listRDD=sc.parallelizePairs(list);
        JavaPairRDD<String,Integer> listRDDResult=listRDD.sortByKey();
        listRDDResult.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+" "+stringIntegerTuple2._2);
            }
        });
    }
}
