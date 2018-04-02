package com.wen.spark.sql.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.mortbay.util.ajax.JSON;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.List;

public class RDDToDataSetProgrammatically {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("RDDToDataSetReflection");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        JavaRDD<String> listRDD=sc.textFile("C:\\Users\\wchen129\\Desktop\\data\\sparkdata\\students.txt");
        //第一步创建RDD  但是需要转换成RDD<Row>
        JavaRDD<Row> rowRDD=listRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] string=s.split(",");
                return RowFactory.create(Integer.parseInt(string[0]),string[1],Integer.parseInt(string[2]));
            }
        });
        //动态构建元数据
        List<StructField> fields=new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));
        StructType structType=DataTypes.createStructType(fields);
        //  第三部将使用动态的元数据，将RDD  转换为DataSet
        Dataset studentFD=sqlContext.createDataFrame(rowRDD,structType);
        //后面就可以使用DataSet
        studentFD.registerTempTable("students");

        Dataset teenagerFD=sqlContext.sql("select * from students where age<19 ");

        List<Row> rows=teenagerFD.javaRDD().collect();
        for(Row row:rows){
            System.out.println(row);
        }

    }
}
