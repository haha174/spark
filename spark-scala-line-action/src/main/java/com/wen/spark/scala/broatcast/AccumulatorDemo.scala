package com.wen.spark.scala.broatcast

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("AccumulatorDemo")
    val sc = new SparkContext(conf)
    val accum = sc.accumulator(0)
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
    println(accum)
  }
}
