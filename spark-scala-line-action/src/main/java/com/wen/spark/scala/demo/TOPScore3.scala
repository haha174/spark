package com.wen.spark.scala.demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

object TOP3 {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setMaster("local").setAppName("TOP3");
      val sc=new SparkContext(conf);
      var lines=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\score.txt");
    val pairs = lines.map { x =>
    {
      val splited = x.split(" ")
      (splited(0), splited(1).toInt)
    }
    }

    val groupedPairs = pairs.groupByKey();


    val top3Score = groupedPairs.map(classScores => {
      val top3 = Array[Int](-1, -1, -1)

      val className = classScores._1

      val scores = classScores._2

      for (score <- scores) {
     breakable{
          for (i <- 0 until 3) {
            if (top3(i) == -1) {
              top3(i) = score;
          break();
            } else if (score > top3(i)) {
              var j = 2
              while (j > i) {
                top3(j) = top3(j - 1);
                j = j - 1
              }
              top3(i) = score;
              break();
            }
          }
     }

      }
      (className, top3);
    })

    top3Score.foreach(x => {
      println(x._1)
      val res = x._2
      for (i <- res) {
        println(i)
      }
      println("==========================")
    })
}
}
