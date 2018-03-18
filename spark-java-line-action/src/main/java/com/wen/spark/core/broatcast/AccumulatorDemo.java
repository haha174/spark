package com.wen.spark.core.broatcast;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class AccumulatorDemo {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("BroatCast").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Accumulator<Integer> sum=sc.accumulator(0);
        List<Integer> listNumber= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rddList=sc.parallelize(listNumber);
       rddList.foreach(new VoidFunction<Integer>(){
            public void call(Integer integer) throws Exception {
                //在java  中 获取广播变量
                sum.add(integer);
            }
    });
       System.out.println(sum.value());
    }
}
