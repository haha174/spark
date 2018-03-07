package com.wen.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Arrays;

public class ActionOperation {
    static SparkConf conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
   static JavaSparkContext sc=new JavaSparkContext(conf);
    public static  void main(String[] args){
      //  reduce();
      //  collect();
      //  count();
      //  take();
       // saveAsTextFile();
        countByKey();
       // foreach();
    }
    public static void reduce(){
        //有一个集合 里面有10个数字   对10个数字进行累加
        //reduce  操作的原理      reduce  首先将第一个和第二个元素传入call  计算  返回结果  比如  1+2=3
        //  将该结果传入到下一个元素进行计算  比如 3+3
        //以此类推

        List<Integer> list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> listRDD=sc.parallelize(list);
       int num= listRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        sc.close();
        System.out.println(num);
    }
    public static void collect(){
        List<Integer> list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> listRdd=sc.parallelize(list);
        JavaRDD<Integer> doubleNumber=listRdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });
        //不使用foreach  action   操作远程集群上的rdd  中的元素
        //而使用 collect 操作   将分布在远程集群上的doubleNumbers RDD  的数据拉取到本地
        //这种方式不建议使用  一般RDD  数据比较大性能比较差
        // 通常还是使用foreach  action  来对最终的RDD  元素进行处理

        List<Integer> list1=doubleNumber.collect();
        for(int i:list1){
            System.out.println(i);
        }
        sc.close();
    }
    public static void count(){
        List<Integer> list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> listRdd=sc.parallelize(list);
        long num=listRdd.count();
        System.out.println(num);
        sc.close();
    }
    public static void take(){
        List<Integer> list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> listRdd=sc.parallelize(list);
        //于 collect 类似但是take  只是获取前n  个数据
        List<Integer> listTakes=listRdd.take(3);
        for(int i:listTakes){
            System.out.println(i);
        }
        sc.close();
    }
    public static void saveAsTextFile(){
        List<Integer> list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> listRdd=sc.parallelize(list);
        //于 collect 类似但是take  只是获取前n  个数据
        listRdd.saveAsTextFile("C://cwq//data//text.txt");
        sc.close();
    }

    public static void countByKey(){
        List<Tuple2<String,Integer>> list= Arrays.asList(
                new Tuple2<String,Integer>("calss1",1)
              , new Tuple2<String,Integer>("calss2",2)
              , new Tuple2<String,Integer>("calss3",3)
                , new Tuple2<String,Integer>("calss3",4)

                , new Tuple2<String,Integer>("calss4",4)
        );
        JavaPairRDD<String,Integer> listRdd=sc.parallelizePairs(list,1);
        //于 collect 类似但是take  只是获取前n  个数据
        Map<String,Long> map= listRdd.countByKey( );
        for (Map.Entry<String,Long> count:map.entrySet() ){
            System.out.println(count.getKey()+"     "+count.getValue());
        }
        sc.close();
    }
    //
    public static void foreach(){
        List<Tuple2<String,Tuple2<String,Integer>>> list= Arrays.asList(
                new Tuple2<String,Tuple2<String,Integer>>("calss1",new Tuple2<>("calss2",1))
                , new Tuple2<String,Tuple2<String,Integer>>("calss3",new Tuple2<>("calss4",1))
                , new Tuple2<String,Tuple2<String,Integer>>("calss5",new Tuple2<>("calss6",1))
                , new Tuple2<String,Tuple2<String,Integer>>("calss7",new Tuple2<>("calss8",1))

                , new Tuple2<String,Tuple2<String,Integer>>("calss9",new Tuple2<>("calss10",1))
        );
        JavaPairRDD<String,Tuple2<String,Integer>> listRdd=sc.parallelizePairs(list);
      listRdd.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Integer>>>() {

          public void call(Tuple2<String,Tuple2<String,Integer>> stringIntegerTuple2) throws Exception {
                    System.out.println(stringIntegerTuple2._1+stringIntegerTuple2._2._1+stringIntegerTuple2._2._2);
          }
      });
        sc.close();
    }
}
