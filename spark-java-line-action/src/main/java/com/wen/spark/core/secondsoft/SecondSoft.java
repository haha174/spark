package com.wen.spark.core.secondsoft;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSoft {
    public static void main(String...args){
        SparkConf conf=new SparkConf().setMaster("local").setAppName("SecondSoftKey");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String > list=sc.textFile("C:\\Users\\haha174\\Desktop\\data\\test.txt");
        JavaPairRDD<SecondSoftKey,String> pairs=list.mapToPair(new PairFunction<String, SecondSoftKey, String>() {
            @Override
            public Tuple2<SecondSoftKey, String> call(String s) throws Exception {
                String str[]=s.split(" ");
                SecondSoftKey secondSoftKey=new SecondSoftKey(Integer.parseInt(str[0]),Integer.parseInt(str[1]));
                return new Tuple2<SecondSoftKey, String>(secondSoftKey,s);
            }
        });
        JavaPairRDD<SecondSoftKey,String> resultPair=pairs.sortByKey();
        JavaRDD<String> result=resultPair.map(new Function<Tuple2<SecondSoftKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondSoftKey, String> secondSoftKeyStringTuple2) throws Exception {
                return secondSoftKeyStringTuple2._2;
            }
        });
        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
