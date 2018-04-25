package com.wen.spark.sql.core

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameStudy {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("DataFrameStudy")
      val  sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
     val reader = sqlContext.read
      val ds = reader.json("hdfs://hadoop:8020/data/students.json")
      ds.show()

  }
}
