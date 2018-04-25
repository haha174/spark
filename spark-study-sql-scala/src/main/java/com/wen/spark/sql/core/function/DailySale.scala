package com.wen.spark.sql.core.function

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DailySale {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("DailyUV").setMaster("local")
    val  sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //1  模拟用户行为日志  第一列是时间，第二列是用户id
    val userAccessLog=Array(
      "2015-10-01,1122,1.0",
      "2015-10-01,1122,2.0",
      "2015-10-01,1123,3.0",
      "2015-10-01,1124,4.0",
      "2015-10-02,1121,5.0",
      "2015-10-02,1122,6.0",
      "2015-10-01,1123,7.0",
      "2015-10-01,1124,8.0");
    //2  创建RDD
    val userAccessLogRDD=sc.parallelize(userAccessLog,5);
    //3 将RDD  转换为DataSet 首先转换元素为Row 的RDD
    val userAccessLogRowRDD=userAccessLogRDD.map(log=>{
      Row(log.split(",")(0),log.split(",")(1).toInt,log.split(",")(2).toDouble)
    })
    //然后构造DataSet元数据
    val structType=StructType(Array(
      DataTypes.createStructField("date", DataTypes.StringType, true),
      DataTypes.createStructField("userId", DataTypes.IntegerType, true),
      DataTypes.createStructField("sale", DataTypes.DoubleType, true)
    ))
    //使用sqlContext  创建DataSet
    import sqlContext.implicits._
    val userAccessLogRowDS=sqlContext.createDataFrame( userAccessLogRowRDD,structType);
    //统计每日的销售额
    userAccessLogRowDS.groupBy("date")
      .agg('date,sum("sale"))//DataSet<Row>
      .show()



    //统计每个用户的销售额
    userAccessLogRowDS.groupBy("userId")
      .agg('userId,sum("sale"))//DataSet<Row>
      .show()


    //统计每个用户每日的销售额
    userAccessLogRowDS.groupBy("userId","date")
      .agg(sum("sale"))//DataSet<Row>
      .show()

  }
}
