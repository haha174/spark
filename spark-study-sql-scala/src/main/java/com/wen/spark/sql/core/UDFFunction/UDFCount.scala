package com.wen.spark.sql.core.UDFFunction

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDFCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("UDF")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    //  构造模拟数据
    val names=Array("Leo","Marry","Jack","Tom","Leo","Marry","Jack","Tom","Leo","Marry","Jack","Tom");
    val namesRDD=sc.parallelize(names);
    val namesRowRDD=namesRDD.map(name=>Row(name))
    val structType= StructType(Array(StructField("name",org.apache.spark.sql.types.StringType,true)))

    var namesDF=sqlContext.createDataFrame(namesRowRDD,structType)
    //注册一张表
    namesDF.registerTempTable("names")
    //自定义函数
    sqlContext.udf.register("strCount",new StringCount())
   val namesREDF=   sqlContext.sql("select name,strCount(name) from names group by name")
    namesREDF   .collect().foreach(row=>println(row.getString(0)+"  "+row.getInt(1)))

    namesREDF.show()
  }
}
