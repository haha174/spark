package com.wen.spark.scala.core
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext

object GroupStr {
  def main(args: Array[String]): Unit = {
    val cf = new SparkConf().setMaster("local").setAppName("GroupStr")
    val sc = new SparkContext(cf)
    var line=Array(new Tuple2[String, Integer]("1", 10), new Tuple2[String, Integer]("2", 20), new Tuple2[String, Integer]("3", 30))
    var listRDD=sc.parallelize(line);
    var listRDDResult=listRDD.groupByKey();
    listRDDResult.foreach(ecore=>{
      print(ecore._1);
      ecore._2.foreach(num=>print(" "+num))
      println()
    })
  }
}
