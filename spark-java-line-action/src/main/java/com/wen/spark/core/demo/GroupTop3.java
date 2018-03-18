package com.wen.spark.core.demo;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//.对每个班级内的学生成绩，取出前三名（分组取topN）
public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TOP3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\score.txt");
        JavaPairRDD<String,Integer> numbers=lines.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String s) throws Exception {
                String str[]=s.split(" ");
                return  new Tuple2<String, Integer>(str[0],Integer.valueOf(str[1]));
            }
        });
        JavaPairRDD<String,Iterable<Integer>> groupByKey=numbers.groupByKey();
        JavaPairRDD<String,Iterable<Integer>> top3Score=groupByKey.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {

            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                Integer[] top3=new Integer[3];
                String name=stringIterableTuple2._1;
                Iterator<Integer> scores=stringIterableTuple2._2.iterator();
                while (scores.hasNext()){
                    Integer score=scores.next();
                    for(int i=0;i<3;i++){
                        if(top3[i]==null){
                            top3[i]=score;
                            break;
                        }else  if(score>top3[i]){
                            for(int j=2;j>i;j--){
                                top3[j]=top3[j-1];
                            }
                            top3[i]=score;
                            break;
                        }

                    }
                }
                return new Tuple2<String, Iterable<Integer>>(name, Arrays.asList(top3));
            }
        });
        top3Score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.print("name   "+stringIterableTuple2._1);
                Iterator<Integer> score=stringIterableTuple2._2.iterator();
                while (score.hasNext()){
                    System.out.print(score.next()+"   ");
                }
                System.out.println();
            }
        });
    }
}
