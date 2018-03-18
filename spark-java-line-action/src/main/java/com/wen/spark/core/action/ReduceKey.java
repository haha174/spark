package com.wen.spark.core.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ReduceKey {
    public static  void  main(String[] args){
        SparkConf cf=new SparkConf().setMaster("local").setAppName("LineCount");
        JavaSparkContext sc=new JavaSparkContext(cf);
        List<Tuple2<String,Integer>> list= Arrays.asList(new Tuple2<String,Integer>("1",10),new Tuple2<String,Integer>("1",20),new Tuple2<String,Integer>("3",30));
        JavaPairRDD<String,Integer> listRDD=sc.parallelizePairs(list);
        JavaPairRDD<String,Integer> listRDDResult=listRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        listRDDResult.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+" "+stringIntegerTuple2._2);
            }
        });
    }
}
