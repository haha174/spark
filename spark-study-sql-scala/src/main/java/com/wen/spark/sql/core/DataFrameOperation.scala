package com.wen.spark.sql.core

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}
;



  object DataFrameOperation {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("DataFrameOperation")
      val sc = new JavaSparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val reader = sqlContext.read
      //创建出dataset  可以理解为一张表
      val ds = reader.json("hdfs://hadoop:8020/data/students.json")
      //打印dataFrame中的所有数据
      ds.show()
      ////打印dataFrame的元数据
      ds.printSchema()
      //查询某列所有的数据
      ds.select("name").show()
      //查询某几列所有的数据并对列进行计算
      ds.select(ds.col("name"), ds.col("age").plus(1)).show()
      //根据某一列的值进行过滤
      ds.filter(ds.col("age").gt("18")).show()
      //根据某一列进行分组聚合
      ds.groupBy(ds.col("age")).count.show()
      sc.close()
    }
}
