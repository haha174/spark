package com.wen.spark.sql.core;

import com.wen.spark.sql.core.Bean.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.mortbay.util.ajax.JSON;
import org.stringtemplate.v4.ST;

/**
 * 使用反射的方式将RDD  转换为DataSet
 */
public class RDDToDataSetReflection {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("RDDToDataSetReflection");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        JavaRDD<String> listRDD=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.json");
        JavaRDD<Student> listStudent=listRDD.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                Student student = com.alibaba.fastjson.JSONObject.parseObject(s,Student.class);
                return student;
            }
        });
        //使用反射的方式将RDD  转成为 DataFrame
        Dataset studentData=sqlContext.createDataFrame(listStudent,Student.class);
        //拿到一个DataSet  后就可以将其注册成为一个零时表，然后针对于之中的数据进行Sql 语句
        studentData.show();
        studentData.registerTempTable("students");
        //针对于 students 执行sql  语句操作
        Dataset teenagerDF=sqlContext.sql("select * from students  where age <19");
        //展示出查询的数据
        teenagerDF.show();
        //将查询出的DataFrame 再一次转化为RDD
        JavaRDD<Row> teenagerRDD=teenagerDF.javaRDD();
        //将RDD  中的数据进行映射转换成为Student
        JavaRDD<Student> result=teenagerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
               Student student=new Student();
               student.setId(row.getInt(1));
                student.setName(row.getString(2));

                student.setAge(row.getInt(0));
                return  student;
            }
        });
        result.foreach(new VoidFunction<Student>() {
            @Override
            public void call(Student student) throws Exception {
                System.out.println(student.toString());
            }
        });
        sc.close();
    }
}
