package com.wen.spark.scala

import org.apache.spark.{SparkConf, SparkContext}
import com.wen.spark.scala.SecondSoft.SecondSoftKey
object SecondSoftDemo {
   def main(args: Array[String]): Unit = {
     val sparkConf =new SparkConf().setAppName("SecondSoft").setMaster("local")
     val sc=new SparkContext(sparkConf);
     val lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\test.txt",1)
     val pairs=lines.map{line=>(
       new SecondSoftKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line
       )}
     val softPairs=pairs.sortByKey();
     var softedLines=softPairs.map(line=>line._2)
     softedLines.foreach(line=>println(line))
   }
}
