package com.wen.spark.streaming.core.updateStateByKey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

public class updateStateByKeyWorldCount {
    public static void main(String[] args) throws InterruptedException {


        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("updateStateByKeyWorldCount");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("hdfs://192.168.1.26:8020/study/spark/checkPoint");
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("192.168.1.26",9999);

        JavaDStream<String> worlds=lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }) ;

        JavaPairDStream<String,Integer> pairs=worlds.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s,1);
            }
        }) ;

        JavaPairDStream<String,Integer> results=pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            //  Optional  代表值的存在状态  有可能存在有可能不存在

            //  实际上对于每个单词每次batch  计算的时候都会调用这个函数
            //  第一个参数  相当于这个batch  中这个key  新的值·
            // 第二个参数就是key  之前的状态

            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                Integer newValue=0;
                //其次判断v2  是否存在
                if(v2.isPresent()){
                    newValue=v2.get();
                }

                //  将本次新出现的值都加到newValue  中
                for(Integer value:v1){
                    newValue+=value;
                }
                return Optional.of(newValue);
            }
        });
        // 到这里为止   相当于的事  每个batch  过来  计算到pairsDstream  就会执行全局的updateStateByKey算子updateStateByKey  返回的JavaPairDStream
        //代表每个key  的全局计数
        results.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();
    }
}
