package com.wen.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object SortKey {
  def main(args: Array[String]): Unit = {
    val cf = new SparkConf().setMaster("local").setAppName("GroupStr")
    val sc = new SparkContext(cf)
    var line = Array(new Tuple2[String, Integer]("1", 50), new Tuple2[String, Integer]("2", 20), new Tuple2[String, Integer]("3", 30))
    var listRDD = sc.parallelize(line);
    var listRDDResult = listRDD.sortByKey(false)
    listRDDResult.foreach(ecore => {
      println(ecore._1 + "   " + ecore._2);
    });
  }
}

