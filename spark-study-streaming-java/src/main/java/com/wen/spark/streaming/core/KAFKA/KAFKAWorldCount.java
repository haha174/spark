package com.wen.spark.streaming.core.KAFKA;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KAFKAWorldCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("KAFKAWorldCount");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String,Integer> map=new HashMap<>();
        map.put("worldCount",1);
        JavaPairReceiverInputDStream<String,String> lines= KafkaUtils.createStream(jssc,"cloud.codeguoj.cn:2181","DefaultConsumerGroup",map);
        JavaDStream<String> worlds=lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return Arrays.asList(stringStringTuple2._2.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,Integer> linePair=worlds.mapToPair(new PairFunction<String, String, Integer>() {
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
        jssc.close();
    }
}
