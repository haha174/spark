package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SaveModelTest
{
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameStudy").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.format("json").load("hdfs://hadoop:8020/data/students.json");
        ds.show();
        //ds.write().format("json").mode(SaveMode.ErrorIfExists).save("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json");
       // ds.write().format("json").mode(SaveMode.Append).save("hdfs://hadoop:8020/data/students.json");
       // ds.write().format("json").mode(SaveMode.Overwrite).save("hdfs://hadoop:8020/data/students.json");
        ds.write().format("json").mode(SaveMode.Ignore).save("hdfs://hadoop:8020/data/students.json");

        sc.close();
    }
}
