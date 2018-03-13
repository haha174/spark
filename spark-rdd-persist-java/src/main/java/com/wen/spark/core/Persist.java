package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Persist {
    static SparkConf conf=new SparkConf().setMaster("local").setAppName("persist");
   static  JavaSparkContext sc=new JavaSparkContext(conf);
    public static void main(String[] args){

       // noCache();
        cache();

    }
    public static void noCache(){

        JavaRDD<String> list=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\test\\hive-site.xml");
        long beginTime=System.currentTimeMillis();
        long count=list.count();
        System.out.println("无持久化第一次"+(System.currentTimeMillis()-beginTime));


        beginTime=System.currentTimeMillis();
        count=list.count();
        System.out.println("无持久化第二次"+(System.currentTimeMillis()-beginTime));
    }
    public static void cache(){

        JavaRDD<String> list=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\test\\hive-site.xml");
        list.cache();
        long beginTime=System.currentTimeMillis();
        long count=list.count();
        System.out.println("持久化第一次"+(System.currentTimeMillis()-beginTime));


        beginTime=System.currentTimeMillis();
        count=list.count();
        System.out.println("持久化第二次"+(System.currentTimeMillis()-beginTime));
    }
}
