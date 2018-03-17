package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
public class flatMapJava {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setMaster("local").setAppName("flatMapJava");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<String> list= Arrays.asList("hello world","hello spark","hello hadoop");
        JavaRDD<String> listRDD=sc.parallelize(list);
        /**
         * flatmap  算子接收的参数是FlatMapFunction  返回 Iterator
         */
        JavaRDD<String> listResultRDD=listRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        listResultRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
