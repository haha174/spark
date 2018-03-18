package com.wen.spark.scala.action

import org.apache.spark.{SparkConf, SparkContext}


object JoinAndCogroup {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("JoinAndCogroup");
    var sc=new SparkContext(conf);
    var studentsList=Array(Tuple2(1,"tom1"),Tuple2(2,"tom2"),Tuple2(3,"tom3"))
    var scoresList=Array(Tuple2(1,10),Tuple2(1,11),Tuple2(2,20),Tuple2(2,21),Tuple2(3,30),Tuple2(3,31))
    var studentRDD=sc.parallelize(studentsList)
    var scoreRDD=sc.parallelize(scoresList)
    var joinRDD=studentRDD.join(scoreRDD)
    joinRDD.foreach(studentScores=>{
      print("id"+studentScores._1)
      print("name"+studentScores._2._1)
      println("score"+studentScores._2._2)
    })
    var cogroupRDD=studentRDD.cogroup(scoreRDD)
    cogroupRDD.foreach(studentScores=>{
      print("id"+studentScores._1)
      print("name"+studentScores._2._1)
      println("score"+studentScores._2._2)
    })
  }
}
