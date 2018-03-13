package com.wen.spark

import org.apache.spark.{SparkConf, SparkContext}

object BroadCast {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("BroadCast")
    val sc=new SparkContext(conf)
    var factor=3
    var fac=sc.broadcast(factor);
    var numAray=Array(1,2,3,4,5)
    var numList=sc.parallelize(numAray)
    var numResult=numList.map(num=>num*fac.value)
    numResult.foreach(num=>println(num))
  }
}
