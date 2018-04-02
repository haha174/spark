package com.wen.spark.sql.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext, SaveMode}

object SaveModelTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameStudy").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val reader = sqlContext.read
    val ds = reader.format("json").load("hdfs://hadoop:8020/data/students.json")
    ds.show()
    //ds.write().format("json").mode(SaveMode.ErrorIfExists).save("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json");
    // ds.write().format("json").mode(SaveMode.Append).save("hdfs://hadoop:8020/data/students.json");
    // ds.write().format("json").mode(SaveMode.Overwrite).save("hdfs://hadoop:8020/data/students.json");
    ds.write.format("json").mode(SaveMode.Ignore).save("hdfs://hadoop:8020/data/students.json")
  }
}
