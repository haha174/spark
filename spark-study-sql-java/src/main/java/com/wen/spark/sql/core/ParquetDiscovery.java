package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class ParquetDiscovery {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("ParquetDiscovery");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.json("hdfs://hadoop:8020/data/gender=male/country=us/users.parquet");
        ds.printSchema();
        ds.show();
       sc.close();
    }
}
