package com.wen.spark.sql.core

import java.util

import com.alibaba.fastjson.JSONObject
import com.wen.spark.sql.core.bean.Student
import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function, VoidFunction}
import org.apache.spark.sql.{Row, SQLContext}

object  RDDToDataSetReflection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RDDToDataSetReflection")
    val sc = new JavaSparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val listRDD = sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json")
    val listStudent = listRDD.map(new Function[String, Student]() {
      @throws[Exception]
      override def call(s: String): Student = {

        val student = com.alibaba.fastjson.JSON.parseObject(s, classOf[Student])

//        var string=s.split(",")
//        var student=new Student;
//        student.setId(Integer.valueOf(string(0).trim()))
//        student.setAge(Integer.valueOf(string(2).trim()))
//        student.setName(string(1))
        student
      }
    })
    //使用反射的方式将RDD  转成为 DataFrame
    val studentData = sqlContext.createDataFrame(listStudent, classOf[Student])
    //拿到一个DataSet  后就可以将其注册成为一个零时表，然后针对于之中的数据进行Sql 语句
    studentData.show()
    studentData.registerTempTable("students")
    //针对于 students 执行sql  语句操作
    val teenagerDF = sqlContext.sql("select * from students  where age <19")
    //展示出查询的数据
    teenagerDF.show()
    //将查询出的DataFrame 再一次转化为RDD
    val teenagerRDD = teenagerDF.javaRDD
    //将RDD  中的数据进行映射转换成为Student
    val result = teenagerRDD.map(new Function[Row, Student]() {
      @throws[Exception]
      override def call(row: Row): Student = {
       new Student(row.getInt(1),row.getString(2),row.getInt(0))
      }
    })
    result.foreach(new VoidFunction[Student]() {
      @throws[Exception]
      override def call(student: Student): Unit = {
        System.out.println(student.toString)
      }
    })
    sc.close()
  }
}
