package com.wen.spark.sql.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ParquetMergeScaema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetMergeScaema")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val reader = sqlContext.read
  //创建一个dataSet  作为学生信息
    import sqlContext.implicits._
    val studentWithNameAndAge=Array(("leo",20),("jack","25")).toSeq
    val studentWithNameAndAgeDF=sc.parallelize(studentWithNameAndAge,2).toDF("name","age");
    studentWithNameAndAgeDF.write.save("hdfs://hadoop:8020/data/ParquetMergeScaemaTest1");


    //再创建一个
    val studentWithNameAndId=Array(("leo",20),("jack","25")).toSeq
    val studentWithNameAndIdDF=sc.parallelize(studentWithNameAndId,2).toDF("name","Id");
    studentWithNameAndIdDF.write.save("hdfs://hadoop:8020/data/ParquetMergeScaemaTest2.json");
    //合并两个dataset
    val student=sqlContext.read.option("mergeSchema","true").parquet("hdfs://hadoop:8020/data/ParquetMergeScaemaResult")
    student.printSchema()
    student.show()
  }
}
