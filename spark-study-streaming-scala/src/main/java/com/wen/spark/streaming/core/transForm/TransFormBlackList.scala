package com.wen.spark.streaming.core.transForm

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
;

object TransFormBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("updateStateByKeyWorldCount").setMaster("local[2]")

    val jssc = new StreamingContext(conf, Durations.seconds(5))
    //首先创建输入DStream   代表一个数据源 （比如kafka  ,socket）  来持续不断的实时数据流
    //调用JavaStreamingContext 的socketTextStream   方法可以创建一个数据源为socket  的网路端口的数据源
    val blackList=Array(("tom",true))
    val blackListRDD=jssc.sparkContext.parallelize(blackList,5);
    val lines = jssc.socketTextStream("www.codeguoj.cn", 9999)

    val userAdsClickLogDStream=lines.map(adfLog=>(adfLog.split(" ")(1),adfLog))

    val validAdfClickRDD=userAdsClickLogDStream.transform(userAdsClickLogRDD=>{
      val joinedRDD=userAdsClickLogRDD.leftOuterJoin(blackListRDD)
      val fliterRDD=joinedRDD.filter(tupe=>{
        if(tupe._2._2.getOrElse(false)){
          false
        }else{
          true
        }
      })
      val clickedLogRDD=fliterRDD.map(tupe=>tupe._2._1)
      clickedLogRDD
    })
    validAdfClickRDD.print()
    jssc.start()
    jssc.awaitTermination()
  }
}
