package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class GenericLoadSave {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameStudy");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.load("hdfs://hadoop:8020/data/students.parquet");
        ds.select("name","age").write().save("hdfs://hadoop:8020/data/studentsSave.parquet");
        sc.close();
    }
}
