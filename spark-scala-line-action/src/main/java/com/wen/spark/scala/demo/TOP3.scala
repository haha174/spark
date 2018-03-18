package com.wen.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object TOP3 {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setMaster("local").setAppName("TOP3");
      val sc=new SparkContext(conf);
      var lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\top3.txt");
      var linesPair=lines.map{lines=>(Integer.valueOf(lines),lines)}
      var softed=linesPair.sortByKey(false);
      val resultRDD=softed.map(lines=>lines._1)
      val result=resultRDD.take(3)
      for ( i<-result)
    println(i)

    }
}
