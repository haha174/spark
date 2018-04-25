package com.wen.spark.sql.core

import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("UDF")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    //  构造模拟数据
    val names=Array("Leo","Marry","Jack","Tom");
    var namesRDD=sc.parallelize(names);
    var namesRowRDD=namesRDD.map(name=>Row(name))
    var structType=StructType(Array(
      DataTypes.createStructField("name", DataTypes.StringType, true)
    ))
    val namesDF=sqlContext.createDataFrame(namesRowRDD,structType)
    //注册一张表
    namesDF.registerTempTable("names")
    //自定义函数
    sqlContext.udf.register("strLen",(str:String)=>str.length)
   val namesREDF=   sqlContext.sql("select name,strLen(name) from names")
    namesREDF   .collect().foreach(row=>println(row.getString(0)+"  "+row.getInt(1)))

    namesREDF.show()
  }
}
