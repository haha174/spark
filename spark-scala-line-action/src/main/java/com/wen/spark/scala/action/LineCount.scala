package com.wen.spark.scala.action

import org.apache.spark.{SparkConf, SparkContext}

object LineCount {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setMaster("local").setAppName("ScalaLineCount");
      var sc=new SparkContext(conf);
      var lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\world-count.txt");
       var pairs=lines.map{line=>(line,1)}
       var lineCounts=pairs.reduceByKey(_+_)
    lineCounts.foreach(lineCount=>println(lineCount._1+"  "+lineCount._2))
  }
}
