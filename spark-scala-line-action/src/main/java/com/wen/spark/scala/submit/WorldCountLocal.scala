package com.wen.spark.scala.submit

import org.apache.spark.{SparkConf, SparkContext}

object SparkOnFile {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WorldCount").setMaster("local");
    val sc = new SparkContext(sparkConf);

    //val textFile = sc.textFile("hdfs://spark1:8020/world-count.txt")
    val textFile = sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\world-count.txt")
    var str="haha wo";
    var str1=str.split(" ");
    var str2=str.split(" ",3);
    var str3=str.split(" ")(0);

    str1.foreach(count=>println("str1+ "+count))
    str2.foreach(count=>println("str2+ "+count))
    str3.foreach(count=>println("str3+ "+count))
    println(str3)
    val counts = textFile.flatMap(line => line.split(" ")(0))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.foreach(count=>println(count._1+" appeard "+count._2+" times"))
    //counts.saveAsTextFile("hdfs://spark1:8020/world-count-result.txt")

  }
}