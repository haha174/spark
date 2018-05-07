package com.wen.spark.streaming.core.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class WindowWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("TransFormBlackList");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaStreamingContext jssc=new JavaStreamingContext(sc, Durations.seconds(5));
        JavaReceiverInputDStream<String> searchLog=jssc.socketTextStream("www.codeguoj.cn",9999);
        // 将搜索词转换成只有一个搜索词
        JavaDStream<String> searchWordDStream=searchLog.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1.split(" ")[0];
            }
        });
        JavaPairDStream<String,Integer> searchWordsPairsDSTream=searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });
        //  第二个参数窗口长度
        //第三个参数  滑动间隔
        //就是说 每个10秒将最近60秒的数据作为一个窗口
       JavaPairDStream<String,Integer> searchWorldCountDStream= searchWordsPairsDSTream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer v1, Integer v2) throws Exception {
               return v1+v2;
           }
       },Durations.seconds(60),Durations.seconds(10));
       //执行transform  操作  根据搜索词进行排序  然后获取排名前三的搜索词

        JavaPairDStream<String,Integer>  finalRDD=  searchWorldCountDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {

                JavaPairRDD<Integer,String> countSearchRDD=v1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                        return new Tuple2<>(stringIntegerTuple2._2,stringIntegerTuple2._1);
                    }
                });
                //然后进行降序排序
                JavaPairRDD<Integer,String> softedRDD=countSearchRDD.sortByKey(false);
                //再一次进行反转
                JavaPairRDD<String,Integer> softedRDDCount=softedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        return new Tuple2<>(integerStringTuple2._2,integerStringTuple2._1);
                    }
                });


             List<Tuple2<String,Integer>> listResult=   softedRDDCount.take(3);
                for(Tuple2<String,Integer> v89:listResult){
                    System.out.println(v89._1+" "  +v89._2);
                }
               return  softedRDDCount;
           }
        });
        finalRDD.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();
    }
}