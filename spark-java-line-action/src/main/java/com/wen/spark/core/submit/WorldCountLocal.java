package com.wen.spark.core.submit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WorldCountLocal {
    public static void main(String[] args){
        /**
         *   第一步 ，创建SparkConf
         * setMaster  设置  集群  master的url   如果设置为local  表示在本地运行
         */
        SparkConf conf=new SparkConf().setAppName("WorldCountLocal").setMaster("local");
        /**
         * 第二步  创建SparkContext  对象
         * 在spark  中SparkContext  是spark  所有功能的入口  无论使用的是java  scala   甚至py  编写都必须有一个SparkContext
         * 它的主要作用包括初始化spark  应用程序所需要的一些核心组件，包括调度器（DAGSchedule,taskScheduler）,他还会去spark  master   节点上去注册等等
         * 但是呢，在spark中编写不同类型的spark 应用程序，使用的SparkContext
         * 如果使用scala  使用  使用原生的SparkContext
         * 如果使用java  那么就是用JavaSparkContext
         * 如果使用Spark Sql   程序  那么就是  SQLContext,HiveContext
         * 如果开发Spark  Streaming   程序  那么就是它独有的SparkContext
         * 以此类推
         */


        JavaSparkContext sc=new JavaSparkContext(conf);
        /**
         * 第三步： 要针对输入源（hdfs文件,本地文件,等等）创建一个初始的RDD
         * 输入源中的数据会被打散，分配到RDD的每个partition  中从而形成一个初始的分布式数据集
         * 本次测试  所以针对本地文件
         * SparkContext   中用于根据文件  类型的输入源创建RDD 的方法，叫做textFile()
         * 在我们这里呢  RDD  中  有元素这种概念    如果是   hdfs  或者本地文件呢   创建RDD 每一个文件就相当于文件里面的一行
         */
        JavaRDD<String>  lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\world-count.txt");
        // 第四步：对初始RDD进行transformation操作，也就是一些计算操作
        // 通常操作会通过创建function，并配合RDD的map、flatMap等算子来执行
        // function，通常，如果比较简单，则创建指定Function的匿名内部类
        // 但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类
        // 先将每一行拆分成单个的单词
        // FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
        // 我们这里呢，输入肯定是String，因为是一行一行的文本，输出，其实也是String，因为是每一行的文本
        // 这里先简要介绍flatMap算子的作用，其实就是，将RDD的一个元素，给拆分成一个或多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("-")) .iterator();
            }
        });




        // 接着，需要将每一个单词，映射为(单词, 1)的这种格式
        // 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
        // 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
        // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
        // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
        // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });
        // 接着，需要以单词作为key，统计每个单词出现的次数
        // 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
        // 比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
        // reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
        // 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
        // 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
        // reduce之后的结果，相当于就是每个单词出现的次数
        /**
         * public interface Function2<T1, T2, R> extends Serializable {
         *R call(T1 var1, T2 var2) throws Exception;
         *}
         *  第三个参数表示返回类型
         */
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        // 到这里为止，我们通过几个Spark算子操作，已经统计出了单词的次数
        // 但是，之前我们使用的flatMap、mapToPair、reduceByKey这种操作，都叫做transformation操作
        // 一个Spark应用中，光是有transformation操作，是不行的，是不会执行的，必须要有一种叫做action
        // 接着，最后，可以使用一种叫做action操作的，比如说，foreach，来触发程序的执行
        wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            private static final long serialVersionUID = 1L;
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }
        });
        sc.close();
    }

}
