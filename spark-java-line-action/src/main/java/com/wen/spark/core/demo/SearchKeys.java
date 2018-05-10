package com.wen.spark.core.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SearchKeys {
    /*
    *该案例中我们假设某搜索引擎公司要统计过去一年搜索频率最高的 K 个科技关键词或词组，
    * 为了简化问题，我们假设关键词组已经被整理到一个或者多个文本文件中，并且文档具有以下格式。
    * spark
    * Spark
    * hadoop
    * HADOOP
    * java
    * 要解决这个问题，首先我们需要对每个关键词出现的次数进行计算，在这个过程中需要识别不同大小写的相同单词或者词组，如”Spark”和“spark” 需要被认定为一个单词。对于出现次数统计的过程和 word count 案例类似；其次我们需要对关键词
    * 或者词组按照出现的次数进行降序排序，在排序前需要把 RDD 数据元素从 (k,v) 转化成 (v,k)；最后取排在最前面的 K 个单词或者词组。
     */
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("SearchKeys").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\SearchKeys.txt");
        JavaPairRDD<String,Integer> linesRDD=lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.toLowerCase(),1);
            }
        });
        JavaPairRDD<String,Integer> reduceRDD=linesRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"  "+stringIntegerTuple2._2);
            }
        });
    }
}
