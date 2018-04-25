package com.wen.spark.sql.core.function

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.api.java.function.Function
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.sql.functions._
object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("DailyUV").setMaster("local")
    val  sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //1  模拟用户行为日志  第一列是时间，第二列是用户id
    val userAccessLog=Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-02,1121",
      "2015-10-02,1122",
      "2015-10-01,1123",
      "2015-10-01,1124");
    //2  创建RDD
    val userAccessLogRDD=sc.parallelize(userAccessLog,5);
    //3 将RDD  转换为DataSet 首先转换元素为Row 的RDD
    val userAccessLogRowRDD=userAccessLogRDD.map(log=>{
      Row(log.split(",")(0),log.split(",")(1).toInt)
    })
    //然后构造DataSet元数据
    val structType=StructType(Array(
      DataTypes.createStructField("date", DataTypes.StringType, true),
      DataTypes.createStructField("userId", DataTypes.IntegerType, true)
    ))
    //使用sqlContext  创建DataSet
    import sqlContext.implicits. _
    val userAccessLogRowDS=sqlContext.createDataFrame( userAccessLogRowRDD,structType);
    userAccessLogRowDS.groupBy("date")
      .agg('date,countDistinct('userId),count('userId))//DataSet<Row>
      .show()

    userAccessLogRowDS.show()
  }
}
