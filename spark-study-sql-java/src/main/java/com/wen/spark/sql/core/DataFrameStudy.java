package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrameReader;

public class DataFrameStudy {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameStudy");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.json("hdfs://hadoop:8020/data/students.json");
        ds.show();
        sc.close();
    }

}
