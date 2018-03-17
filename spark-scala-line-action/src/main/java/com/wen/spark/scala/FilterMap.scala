package com.wen.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object FilterMap {
  def main(args: Array[String]): Unit = {
      def conf=new SparkConf().setMaster("local").setAppName("FliterMap")
      def sc=new SparkContext(conf);
      var list=Array("hello world", "hello spark", "hello hadoop")
      var strRDD=sc.parallelize(list);
     var strRDDResult=strRDD.flatMap(line=>line.split(" "))
    strRDDResult.foreach(line=>println(line))
  }
}
