package com.wen.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WorldCountSoft {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WorldCount").setMaster("local");
    val sc = new SparkContext(sparkConf);

    //val textFile = sc.textFile("hdfs://spark1:8020/world-count.txt")
    val textFile = sc.textFile("C:\\Users\\wchen129\\Desktop\\test\\test.txt");
    var counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
   val countWorld= counts.map(word=>(word._2,word._1))
    val softWorld=  countWorld .sortByKey(false)
    softWorld.foreach(count=>println(count._1+" appeard "+count._2+" times"))
    //counts.saveAsTextFile("hdfs://spark1:8020/world-count-result.txt")
  }
}