package com.wen.spark.core.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.List;

import javax.swing.*;

public class TOP3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TOP3").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\top3.txt");
        //  第一个是call 的参数类型  第二个  第三个是   Tuple2的类型。
        JavaPairRDD<Integer,String> numbers=lines.mapToPair(new PairFunction<String, Integer, String>() {

            public Tuple2<Integer, String> call(String s) throws Exception {
                return  new Tuple2<Integer, String>(Integer.valueOf(s),s);
            }
        });
        JavaPairRDD<Integer,String> softed=numbers.sortByKey(false);
        JavaRDD<Integer> resultRDD=softed.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2._1;
            }
        });
       List<Integer> result=resultRDD.take(3);
       for (Integer i:result){
           System.out.println(i);
       }
        sc.close();
    }
}
