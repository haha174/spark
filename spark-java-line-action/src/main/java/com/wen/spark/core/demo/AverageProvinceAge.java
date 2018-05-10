package com.wen.spark.core.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Iterator;

public class AverageProvinceAge {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("AverageProvinceAge").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        /*
         *1,shanghai,12
         * 2,jiangsu,14
         */
        JavaRDD<String> lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\ageProvice.txt");
        long count = lines.count();
        JavaPairRDD<String,Integer> ageRDD=lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] str=s.split(",");
                return new Tuple2<>(str[1],Integer.parseInt(str[2]));
            }
        });
        JavaPairRDD<String,Iterable<Integer>>  groupedRDD=   ageRDD.groupByKey();
        JavaPairRDD<String,Integer> countRDD=groupedRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                Iterator<Integer> list=stringIterableTuple2._2.iterator();
                int count=0;
                int sum=0;
                while (list.hasNext()){
                    sum=sum+list.next();
                    count++;
                }
                return new Tuple2<>(stringIterableTuple2._1,sum/count);
            }
        });
        countRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"  "+stringIntegerTuple2._2);
            }
        });
    }
}
