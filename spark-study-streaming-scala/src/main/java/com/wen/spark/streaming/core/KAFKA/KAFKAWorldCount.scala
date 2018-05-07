//package com.wen.spark.streaming.core.KAFKA
//
//import java.util
//import java.util.{Arrays, HashMap, Iterator, Map}
//
//import org.apache.spark.SparkConf
//import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
//import org.apache.spark.streaming.{Durations, StreamingContext}
//import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream, JavaPairReceiverInputDStream, JavaStreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils
//
//object KAFKAWorldCount {
//  @throws[InterruptedException]
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("KAFKAWorldCount")
//    val jssc = new StreamingContext(conf, Durations.seconds(5))
//    val map = new util.HashMap[String, Integer]
//    map.put("worldCount", 1)
//    val lines = KafkaUtils.createStream(jssc, "cloud.codeguoj.cn:2181", "DefaultConsumerGroup", map)
//
//
//    jssc.start()
//    jssc.awaitTermination()
//    jssc.stop()
//    jssc.close()
//  }
//}
