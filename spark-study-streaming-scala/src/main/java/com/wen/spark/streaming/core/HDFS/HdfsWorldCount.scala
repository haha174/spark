package com.wen.spark.streaming.core.HDFS

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}

object HdfsWorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSWorldCount").setMaster("local[2]")
    val jssc = new StreamingContext(conf, Durations.seconds(5))
    val lines = jssc.textFileStream("hdfs://192.168.1.26:8020/study/spark/streaming")

    val counts = lines.flatMap(line => line.split(" "))  .map(word => (word, 1))
      .reduceByKey(_ + _)
      counts.print()
    jssc.start()
    jssc.awaitTermination()
    jssc.stop()
  }
}
