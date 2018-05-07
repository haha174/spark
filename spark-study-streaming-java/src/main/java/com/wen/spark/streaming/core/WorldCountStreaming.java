package com.wen.spark.streaming.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WorldCountStreaming {
    public static void main(String[] args) throws InterruptedException {
        //创建world
        //但是这里有一点不同，我们是要给他设置一个master  属性，但是我们测试的时候用local  模式
        //local  后面需要跟一个方括号，数字代表了，我们使用几个线程来执行我们的spark  streaming  程序

        SparkConf conf=new SparkConf().setAppName("WorldCountStreaming").setMaster("local[2]");
        //创建JavaStreamingContext
        //该对象就类似于spark  core  中的JavaSparkContext
        //该对象除了接收sparkConf  对象之外还必须接收一个batch interval  参数，就是说 每收集多长时间的数据，划分一个batch  ，进行处理
        //这里设置一秒
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
        //首先创建输入DStream   代表一个数据源 （比如kafka  ,socket）  来持续不断的实时数据流
        //调用JavaStreamingContext 的socketTextStream   方法可以创建一个数据源为socket  的网路端口的数据源
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("www.codeguoj.cn",9999);
        //到这里为止可以理解为JavaReceiverInputDStream 中每隔一秒会由一个RDD  其中封装了这一秒发过来的数据。
        //RDD  的数据类型为String
        //接下来就是world  count
        JavaDStream<String> worlds=lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("-")).iterator();
            }
        }) ;

        JavaPairDStream<String,Integer> pairs=worlds.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s,1);
            }
        }) ;

        JavaPairDStream<String,Integer> results=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        results.print();


        Thread.currentThread().sleep(5000);
        //但是一定要注意JavaStreamingContext 的计算模型就决定了我们必须自己来进行中间缓存的控制比如写入redis
        //他的计算模型和storem  是完全不同的storm  是自己编写的一个一个程序运行在节点上，相当于一个对象 可以自己在对象中控制缓存
        //但是spark   本身是函数式编程的模型，所以比如在worlds  或者pairs  或者DStream  没法在实例变量中缓存
        //  此时执行将最后计算出来的results 中一个一个RDD  写入外部的缓存  或者持久化DB
        //JavaStreamingContext  后续处理
        //必须调用 jssc.start(); 整个 Streaming app  才会执行
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
