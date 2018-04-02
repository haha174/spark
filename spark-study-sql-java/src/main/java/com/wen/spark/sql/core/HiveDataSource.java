package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class HiveDataSource {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("HiveDataSource");
        JavaSparkContext sc=new JavaSparkContext(conf);


    }
}
