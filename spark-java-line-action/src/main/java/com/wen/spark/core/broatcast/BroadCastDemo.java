package com.wen.spark.core.broatcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadCastDemo {
    public static void  main(String[] args){
        SparkConf conf=new SparkConf().setAppName("BroatCast").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> listNumber= Arrays.asList(1,2,3,4,5);
        final int factor=3;
        //在java  中 创建广播变量
        final Broadcast<Integer> factoryBroatCast=sc.broadcast(factor);

        JavaRDD<Integer> rddList=sc.parallelize(listNumber);
        JavaRDD<Integer> result=rddList.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                //在java  中 获取广播变量
               return integer*factoryBroatCast.value();
            }
        });
        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
