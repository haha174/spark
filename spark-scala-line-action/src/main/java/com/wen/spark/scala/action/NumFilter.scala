package com.wen.spark.scala.action

import org.apache.spark.{SparkConf, SparkContext}

object NumFilter {
  def main(args: Array[String]): Unit = {
      var conf=new SparkConf().setMaster("local").setAppName("NumFilter");
      var sc=new SparkContext(conf);
       var  listNumber=Array(1,2,3,4,5,6,7,8,9,10);
      var numberRDD=sc.parallelize(listNumber);
      var filterNumberRDD=numberRDD.filter(num=>num%2==0)
      filterNumberRDD.foreach(num=>println(num))
  }
}
