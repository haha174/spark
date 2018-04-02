package com.wen.spark.sql.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}

object GenericLoadSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameStudy")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val reader = sqlContext.read
    val ds = reader.load("hdfs://hadoop:8020/data/students.parquet")
    ds.select("name", "age").write.save("hdfs://hadoop:8020/data/studentsSave.parquet")
  }
}
