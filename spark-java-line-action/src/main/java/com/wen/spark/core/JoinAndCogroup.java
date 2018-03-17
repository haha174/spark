package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple1;
import scala.Tuple2;
import java.util.*;

public class JoinAndCogroup {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("JoinAndCogroup").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        /**
         * 模拟结合
         */
        List<Tuple2<Integer,String>> studentList=Arrays.asList(
                new Tuple2<Integer,String>(1,"tom1"),
                new Tuple2<Integer,String>(2,"tom2"),
                new Tuple2<Integer,String>(3,"tom3")
        );
        List<Tuple2<Integer,Integer>> scoreList=Arrays.asList(
                new Tuple2<Integer,Integer>(1,10),
                new Tuple2<Integer,Integer>(2,20),
                new Tuple2<Integer,Integer>(3,30),
                new Tuple2<Integer,Integer>(1,40),
                new Tuple2<Integer,Integer>(2,50),
                new Tuple2<Integer,Integer>(3,60)
        );
        //并行化两个RDD
        JavaPairRDD<Integer,String> studentRDD=sc.parallelizePairs(studentList);
        JavaPairRDD<Integer,Integer> scoreRDD=sc.parallelizePairs(scoreList);
        //  使用join 算子关联两个RDD
        JavaPairRDD<Integer,Tuple2<String,Integer>> studengScore=studentRDD.join(scoreRDD);
        //  返回 的 是 join  后的 JavaPairRDD   第一个是两个共同的key
        //  Tuple2  第一个 studentList String
        //              scoreList  Integer


        studengScore.foreach(
                new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
                    public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                        System.out.print("id="+integerTuple2Tuple2._1);
                        System.out.print("name="+integerTuple2Tuple2._2()._1);
                        System.out.println("score="+integerTuple2Tuple2._2()._2);
                    }
                }
        );

/*
        JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>> studengScore=studentRDD.cogroup(scoreRDD);
        //  一个key  join  上的所有value  都放到一个Iterable
        studengScore.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2) throws Exception {
                System.out.print("id="+integerTuple2Tuple2._1);
                System.out.print("name="+integerTuple2Tuple2._2()._1);
                System.out.println("score="+integerTuple2Tuple2._2()._2);
            }
        });*/

        sc.close();
    }
}
