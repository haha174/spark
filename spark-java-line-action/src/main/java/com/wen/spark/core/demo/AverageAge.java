package com.wen.spark.core.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class AverageAge {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("AverageAge").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\age.txt");
        /*
         *1 12
         * 2 14
         * 3 15
         */
        long count = lines.count();
        JavaRDD<Integer> ageRDD=lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return Integer.parseInt(v1.split(",")[1]);
            }
        });


       int ageSum= ageRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println(ageSum/count);
    }
}
