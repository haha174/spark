package com.wen.spark.streaming.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;

public class Top3HotProduct {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("Top3HotProduct").setMaster("local[2]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(1));
        //首先看一下，输入日志的格式
        //leo  product1 category1
        //首先获取输入数据
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("www.codeguoj.cn",9999);
        JavaPairDStream<String,Integer> categoryProductDStream=lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] prudoctSplited=s.split(" ");

                return new Tuple2<>(prudoctSplited[2]+"-"+prudoctSplited[1],1);
            }
        });
        //然后执行window
        //到这里，就可以做到，每隔10秒钟，对最近60秒的数据，执行reduceByKey  操作
        //计算出来这60秒内，每个种类的每个商品的点击次数
        JavaPairDStream<String,Integer> categoryProductDStreamed=categoryProductDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        },Durations.seconds(60),Durations.seconds(10));
        //然后针对60秒内的每个种类的每个商品的点击次数
        categoryProductDStreamed.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                JavaRDD<Row> rowCategoryCount=    stringIntegerJavaPairRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> v1) throws Exception {
                        String category=v1._1.split("-")[0];
                        String product=v1._1.split("-")[1];
                        int count=v1._2;
                        return RowFactory.create(category,product,count);
                    }
                });
                //DataSet  转换
                List<StructField> structFields=new ArrayList<>();
                structFields.add(DataTypes.createStructField("category",DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("product",DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("click_count",DataTypes.IntegerType,true));
                StructType structType=DataTypes.createStructType(structFields);
                SQLContext sqlContext=new SQLContext(rowCategoryCount.context());
                Dataset cataCountDS=sqlContext.createDataFrame(rowCategoryCount,structType);
                //  将60秒内的数据创建一个零时表
                cataCountDS.registerTempTable("product_click_log");
                Dataset cataSearchDS=   sqlContext.sql(
                        "SELECT category,product,click_count "
                                + "FROM ("
                                + "SELECT "
                                + "category,"
                                + "product,"
                                + "click_count,"
                                + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
                                + "FROM product_click_log"
                                + ") tmp "
                                + "WHERE rank<=3");
                cataSearchDS.show();
            }
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();
    }
}
