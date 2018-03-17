package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class TransOpeNum {
    public static  void  main(String[] args){
        map();

    }
    private static void map(){
        /**8
         * 使用map  算子将集合中的每个元素都乘以2
         * map  算子对任何类型的RDD  都可以调用
         * 在java  中  map  算子接收的是Function对象
         * 创建function  一定会让你设置第二个泛型参数，这个泛型类型，就是返回的新元素的类型
         * 在call  方法中的返回类型  也必须是 与第二个泛型同步
         * 在call  方法的内部就可以原始RDD 中每个元素进行计算返回新的元素  组成新的RDD
         */
        SparkConf conf=new SparkConf();
        conf.setAppName("TransOpeNum");
        conf.setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        /**
         * 创建RDD
         */
        JavaRDD<Integer>  numberRDD=sc.parallelize(numbers);
        /**
         * map  算子
         */
        JavaRDD<Integer>  multiplenumberRDDs=numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });
        multiplenumberRDDs.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
