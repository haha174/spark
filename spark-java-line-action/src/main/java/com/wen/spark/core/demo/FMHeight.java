package com.wen.spark.core.demo;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

import static sun.misc.Version.print;

public class FMHeight {
    /*
    *本案例假设我们需要对某个省的人口 (1 亿) 性别还有身高进行统计，
    * 需要计算出男女人数，男性中的最高和最低身高，以及女性中的最高和最低身高。
    * 本案例中用到的源文件有以下格式, 三列分别是 ID，性别，身高 (cm)。
    * 1,F,176a
    * 2,m,160
     */
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("FMHeight");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\FMHeight.txt");
        JavaPairRDD<String ,Integer> pairRDD =lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String str[]=s.split(",");
                return new Tuple2<String ,Integer>(str[1],Integer.parseInt(str[2]));
            }
        });



        long startTime1=System.currentTimeMillis();


        JavaPairRDD<String ,Iterable<Integer>> groupedRDD =pairRDD.groupByKey();
        JavaPairRDD<String,String> resultRDD=groupedRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {

                int max=0;
                int min=Integer.MAX_VALUE;
                Iterator<Integer> list=stringIterableTuple2._2.iterator();

                while (list.hasNext()){
                    int temp=list.next();
                    if(temp>max){
                        max=temp;
                    }
                    if(temp<min){
                        min=temp;
                    }
                }
                return new Tuple2<>(stringIterableTuple2._1,max+","+min);
            }
        });
        resultRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] str=stringStringTuple2._2.split(",");
                System.out.println("sex："+stringStringTuple2._1+"  max："+str[0]+"  min："+str[1]);
            }
        });

        System.out.println("========================================"+(System.currentTimeMillis()-startTime1));












        long startTime=System.currentTimeMillis();
        JavaPairRDD<String ,Integer> pairFRDD=   pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                if(v1._1.equals("F")){
                    return true;
                }else {
                    return false;
                }
            }
        });





        JavaPairRDD<String ,Integer> pairMRDD=   pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                if(v1._1.equals("M")){
                    return true;
                }else {
                    return false;
                }
            }
        });

        Tuple2<String ,Integer>  pairMRDDSoftEDMMax= pairMRDD.sortByKey().first();
        Tuple2<String ,Integer>  pairMRDDSoftEDMMin= pairMRDD.sortByKey(false).first();;

        Tuple2<String ,Integer>  pairMRDDSoftFMax= pairMRDD.sortByKey().first();;
        Tuple2<String ,Integer>  pairMRDDSoftFMin= pairMRDD.sortByKey(false).first();;

        System.out.println("sex："+pairMRDDSoftEDMMax._1+"  max："+pairMRDDSoftEDMMax._2+"  min："+pairMRDDSoftEDMMin._2);


        System.out.println("sex："+pairMRDDSoftFMax._1+"  max："+pairMRDDSoftFMax._2+"  min："+pairMRDDSoftFMin._2);


        System.out.println("========================================"+(System.currentTimeMillis()-startTime));







    }

}
