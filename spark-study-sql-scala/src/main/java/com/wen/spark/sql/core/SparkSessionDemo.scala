package com.wen.spark.sql.core

import org.apache.spark.sql.SparkSession

object SparkSessionDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkSqlDemo")
      .master("local")
      .config("spark.sql.warehouse.dir","C:\\Users\\wchen129\\Desktop\\data\\sparkdata")
      .getOrCreate()
      //
    var df=spark.read.json("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json")
    df.show()
    df.printSchema()
    df.select("name").show()

  }
}
