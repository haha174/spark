package com.wen.spark.streaming.core.HDFS;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class HDFSWorldCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("HDFSWorldCount").setMaster("local[2]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> lines=jssc.textFileStream("hdfs://192.168.1.26:8020/study/spark/streaming");
        JavaDStream<String> lineFlat=  lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,Integer> linePair=lineFlat.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairDStream<String,Integer> lineWorldCount=linePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        lineWorldCount.print();

        jssc.start();

        jssc.awaitTermination();
        jssc.stop();
    }
}
