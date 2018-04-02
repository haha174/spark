package com.wen.spark.sql.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrameReader, SQLContext}

object ManuallySpecifyOptions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameStudy").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val reader = sqlContext.read
    val ds = reader.format("json").load("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json")
    ds.show()
    ds.write.save("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.parquet")

  }
}
