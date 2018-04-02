package com.wen.spark.sql.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}

object ParquetDiscovery {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetDiscovery")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val reader = sqlContext.read
    val ds = reader.json("hdfs://hadoop:8020/data/gender=male/country=us/users.parquet")
    ds.printSchema()
    ds.show()
  }
}
