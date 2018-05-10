package com.wen.spark.streaming.core.KAFKA;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class KAFKADriectWorldCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("KAFKAWorldCount");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String,String> map=new HashMap<>();
        map.put("metadata.broker.list","www.codeguoj.cn:9092");
        Set<String> toPics=new HashSet<>();
        toPics.add("worldCount");
        JavaPairInputDStream<String,String> lines= KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                map,
                toPics);
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
