package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * spark  集群部署
 */
public class WorldCountLocal {
    public static void main(String[] args){
        /**
         * 如果提交集群只需要修改两个地方
         * 第一删除setMaster
         * 第二  hadf  上面的文件
         */
        SparkConf conf=new SparkConf().setAppName("WorldCountLocal");
        /**
         *
         * 1  将world-count.txt  上传到hdfs
         * 2  maven 打包
         * 3 编写提交脚本
         */
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String>  lines=sc.textFile("hdfs://spark1:8020/world-count.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("-")) .iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            private static final long serialVersionUID = 1L;
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }
        });
        sc.close();
    }
}
