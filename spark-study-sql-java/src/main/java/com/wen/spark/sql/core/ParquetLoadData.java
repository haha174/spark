package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import java.util.List;
import org.apache.spark.sql.Row;

public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("ParquetLoadData").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        System.setProperty("HADOOP_USER_NAME", "root");
        sc.hadoopConfiguration().set("dfs.client.use.datanode.hostname","true");
        SQLContext sqlContext=new SQLContext(sc);
        DataFrameReader reader=sqlContext.read();
        Dataset ds= reader.load("hdfs://cloud.codeguoj.cn:8020/test/parquet/");
        ds.show();
        ds.registerTempTable("users");
        Dataset userName=sqlContext.sql("select name from users");
        //对查询出来的dataSet 进行操作，处理打印
        List<String> userNameRDD=userName.javaRDD().map(new Function<Row,String>() {
            public String call(Row o) throws Exception {
                return "name:"+o.getString(0);
            }
        }).collect();
        for(String usernmae:userNameRDD){
            System.out.println(usernmae);
        }
    }
}
