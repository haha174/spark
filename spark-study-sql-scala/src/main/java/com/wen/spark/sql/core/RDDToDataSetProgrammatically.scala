package com.wen.spark.sql.core

import java.util
import java.util.{ArrayList, List}

import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.Function
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
;

object RDDToDataSetProgrammatically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RDDToDataSetReflection")
    val sc = new JavaSparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits. _
    val listRDD = sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.txt")
    //第一步创建RDD  但是需要转换成RDD<Row>
    val rowRDD = listRDD.map(new Function[String, Row]() {
      @throws[Exception]
      override def call(s: String): Row = {
        val string = s.split(",")
        Row(Integer.parseInt(string(0)), string(1),Integer.parseInt( string(2)))
      }
    })
    //动态构建元数据
    val fields = new util.ArrayList[StructField]
    fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true))
    fields.add(DataTypes.createStructField("name", DataTypes.StringType, true))
    fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true))
    val structType = DataTypes.createStructType(fields)
    //  第三部将使用动态的元数据，将RDD  转换为DataSet
    val studentFD = sqlContext.createDataFrame(rowRDD, structType)
    //后面就可以使用DataSet
    studentFD.registerTempTable("students")
    val teenagerFD = sqlContext.sql("select * from students where age<19 ")
    val rows = teenagerFD.javaRDD.collect
    import scala.collection.JavaConversions._
    for (row <- rows) {
      System.out.println(row)
    }
  }
}
