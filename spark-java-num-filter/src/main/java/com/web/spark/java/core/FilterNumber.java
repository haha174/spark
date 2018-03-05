package com.web.spark.java.core;

import org.apache.commons.lang3.JavaVersion;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class FilterNumber {
    public static void main(String[] args){
        SparkConf cf =new SparkConf().setMaster("local").setAppName("FilterNumber");
        JavaSparkContext sc=new JavaSparkContext(cf);
        List<Integer> FilterNumbers= Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD=sc.parallelize(FilterNumbers);
        /**
         * 过滤其中的偶数
         * filter 算子 传入的也是function  但是和map  有区别的是返回的类型是boolean  类型
         * 每一个初始RDD 元素都会传入call  判断逻辑自己编写  如果想保留返回true  不想保留返回false
         *
         */
        JavaRDD<Integer> filterRDD=numberRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer%2==0?true:false;
            }
        });
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {

                System.out.println(integer);
            }
        });
        sc.close();
    }
}
