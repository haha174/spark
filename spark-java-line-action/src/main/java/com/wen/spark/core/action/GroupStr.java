package com.wen.spark.core.action;

import com.sun.xml.internal.ws.api.pipe.Tube;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

import java.util.Arrays;

public class GroupStr {
    public static  void  main(String[] args){
        SparkConf cf=new SparkConf().setMaster("local").setAppName("LineCount");
        JavaSparkContext sc=new JavaSparkContext(cf);
        List<Tuple2<String,Integer>> list= Arrays.asList(new Tuple2<String,Integer>("1",10),new Tuple2<String,Integer>("2",20),new Tuple2<String,Integer>("3",30));
        JavaPairRDD<String,Integer> listRDD=sc.parallelizePairs(list);
        JavaPairRDD<String,Iterable<  Integer>> listRDDResult=listRDD.groupByKey();
        listRDDResult.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.print(stringIterableTuple2._1);
                Iterator<Integer> inte= stringIterableTuple2._2.iterator();
                while (inte.hasNext()){
                    System.out.print(" "+inte.next());
                }
                System.out.println();
            }
        });
    }
}
