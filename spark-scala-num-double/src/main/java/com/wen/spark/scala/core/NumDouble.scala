package com.wen.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object NumDouble {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("NumDouble").setMaster("local")
      val sc=new SparkContext(conf)
      val numbers=Array(1, 2, 3, 4, 5)
      val numberRDD=sc.parallelize(numbers)
      var muleNumberRDD=numbers.map(num=>num*2)
      muleNumberRDD.foreach(num=>println(num))
  }
}
