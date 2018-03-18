package com.wen.spark.core.submit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WorldCountSoft {
    public static void main(String[] args){
        SparkConf cf=new SparkConf().setAppName("WorldCountSoft").setMaster("local");

        JavaSparkContext sc=new JavaSparkContext(cf);
        JavaRDD<String> rddList=sc.textFile("C:\\Users\\wchen129\\Desktop\\test\\test.txt");
        JavaRDD<String> words=rddList.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });



        //按照单词出现次数降序排序。
        //进行ley -value 的反转映射
        // 需要将其反转成 （1，hello  格式）
        JavaPairRDD<Integer,String> resultBefore=wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return       new Tuple2<Integer, String>(stringIntegerTuple2._2,stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer,String> result=resultBefore.sortByKey(false);
        result.foreach(new VoidFunction<Tuple2<Integer,String>>() {
            public void call(Tuple2<Integer,String> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._2+"     "+stringIntegerTuple2._1);
            }
        });
        sc.close();



    }
}
