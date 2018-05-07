package com.wen.spark.streaming.core.transForm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TransFormBlackList {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("TransFormBlackList");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaStreamingContext jssc=new JavaStreamingContext(sc, Durations.seconds(5));
        //  用户对我们的网站上面的广告可以进行点击
        //点击之后，要进行实时的计费，点一下，算一次钱
        //但是对于某些无良商家刷广告的人，那么我们就有一个黑名单
        //只要是黑名单中点击的都过滤掉
        //  日志格式  date  username
        //  先做一份模拟的黑名单
        List<Tuple2<String,Boolean>> blackListData=new ArrayList<>();
        blackListData.add(new Tuple2<>("tom",true));
       final JavaPairRDD<String,Boolean> blackListRDD=sc.parallelizePairs(blackListData);
        JavaReceiverInputDStream<String> adsClickLog=jssc.socketTextStream("www.codeguoj.cn",9999);
        //  首先需要转换一个格式(date,date username)
        //以便于后面 于黑名单RDD  进行join
        JavaPairDStream<String,String> userAdsClickDStream=adsClickLog.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1],s);
            }
        });
        //执行transform  操作实时进行黑名单过滤

        JavaDStream<String> validAdfClickLogDStream= userAdsClickDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
           private static final  long serialVersionUID=1L;
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> v1) throws Exception {
                //  并不是每个用户都存在给名单中所以使用做外链接

                JavaPairRDD<String, Tuple2<String, org.apache.spark.api.java.Optional<Boolean>>> joindRDD= v1.leftOuterJoin(blackListRDD);
                //这里得到的是用户的点击日志  和在黑名单的状态
                JavaPairRDD<String, Tuple2<String, org.apache.spark.api.java.Optional<Boolean>>> filterRDD=joindRDD.filter(new Function<Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<Boolean>>>, Boolean>() {
                    private static final  long serialVersionUID=1L;
                    @Override

                    public Boolean call(Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<Boolean>>> v1) throws Exception {
                        if(v1._2._2().isPresent()&&v1._2._2().get()){
                            return false;
                        }
                        return true;
                    }
                });
                JavaRDD<String> validAdfClickRDD=filterRDD.map(new Function<Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<Boolean>>> v1) throws Exception {
                        return v1._2._1;
                    }
                });
                return validAdfClickRDD;

            }
        });
        validAdfClickLogDStream.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
