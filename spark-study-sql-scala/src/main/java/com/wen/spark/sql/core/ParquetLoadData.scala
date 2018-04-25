package com.wen.spark.sql.core

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.sql._

import scala.collection.immutable.List
object ParquetLoadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetLoadData")
    val sc = new JavaSparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val reader = sqlContext.read
    val ds = reader.json("hdfs://hadoop:8020/data/users.parquet")
    ds.show()
    ds.registerTempTable("users")
    val userName = sqlContext.sql("select name from users")
    //RDD<ROW>
    import scala.collection.JavaConversions._
    //对查询出来的dataSet 进行操作，处理打印
  val userNameRDD = userName.javaRDD.map(new Function[Row, String]() {
    @throws[Exception]
    override def call(o: Row): String = {
      return "name:" + o.getString(0)
    }
  }).collect()

    for (usernmae <- userNameRDD) {
      println(usernmae)
    }
  }
}
