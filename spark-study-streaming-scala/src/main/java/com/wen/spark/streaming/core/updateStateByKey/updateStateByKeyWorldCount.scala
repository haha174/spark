package com.wen.spark.streaming.core.updateStateByKey

import org.apache.spark.SparkConf
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object updateStateByKeyWorldCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName("updateStateByKeyWorldCount").setMaster("local[2]")
    //创建JavaStreamingContext
    //该对象就类似于spark  core  中的JavaSparkContext
    //该对象除了接收sparkConf  对象之外还必须接收一个batch interval  参数，就是说 每收集多长时间的数据，划分一个batch  ，进行处理
    //这里设置一秒
    val jssc = new StreamingContext(conf, Durations.seconds(5))
    //首先创建输入DStream   代表一个数据源 （比如kafka  ,socket）  来持续不断的实时数据流
    //调用JavaStreamingContext 的socketTextStream   方法可以创建一个数据源为socket  的网路端口的数据源
    jssc.checkpoint("hdfs://192.168.1.26:8020/study/spark/checkPoint")
    val lines = jssc.socketTextStream("192.168.1.26", 9999)
    val worlds = lines.flatMap(line=>line.split(" "))
    val pairs = worlds.map(pair=>(pair,1))
    val results= pairs.updateStateByKey((values:Seq[Int],state:Option[Int])=>{

      var newValue:Int=state.getOrElse(0)

      for(value<-values){
        newValue+= value
      }
      Option(newValue)
    })
    results.print()
    jssc.start()
    jssc.awaitTermination()
  }
}
