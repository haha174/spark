package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class ManuallySpecifyOptions {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameStudy").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.format("json").load("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json");
        ds.show();
        ds.write().save("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.parquet");
        sc.close();
    }
}
