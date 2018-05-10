package com.wen.spark.sql.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class GenericLoadSave {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkConf conf=new SparkConf().setAppName("DataFrameStudy").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        Configuration configuration=sc.hadoopConfiguration();
        configuration.set("dfs.client.use.datanode.hostname","true");
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.format("json").load("hdfs://cloud.codeguoj.cn:8020/data/students.json");
        ds.show();
        ds.select("name","age").write().format("parquet").mode(SaveMode.Overwrite).save("hdfs://cloud.codeguoj.cn:8020/test/parquet/");
        sc.close();
    }
}
