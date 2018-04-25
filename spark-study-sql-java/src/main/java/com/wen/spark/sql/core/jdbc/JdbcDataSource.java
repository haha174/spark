package com.wen.spark.sql.core.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.*;


public class JdbcDataSource {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("JdbcDataSource");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        //在两张表中分别取出  转换为  Dataset
        Map<String,String> options=new HashMap<String,String>();
        options.put("url","jdbc:mysql://haha174:3306/test");
        options.put("dbtable","students_infos");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user","root");
        options.put("password","root");
        Dataset studentsDS=sqlContext.read().format("jdbc").options(options).load();
        options.clear();
        options.put("url","jdbc:mysql://haha174:3306/test");
        options.put("dbtable","students_scores");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user","root");
        options.put("password","root");
        Dataset scoreDS =sqlContext.read().format("jdbc").options(options).load();
        //将两个DataSet  转换为JavaRDD
        JavaPairRDD<String,Tuple2<Integer,Integer>> studentRDD=studentsDS.javaRDD().mapToPair(new PairFunction<Row ,String,Integer>() {
            private static final long serialVersionUID=1L;
            @Override
            public Tuple2<String,Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(1),row.getInt(2));
            }
        }).join(scoreDS.javaRDD().mapToPair(new PairFunction<Row ,String,Integer>() {
            private static final long serialVersionUID=1L;

            public Tuple2<String,Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(1),row.getInt(2));
            }
        }));

        //将JavaRDD  转换为JavaRDD<Row>
        JavaRDD<Row> StudentRowRDD=studentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                return RowFactory.create(stringTuple2Tuple2._1,stringTuple2Tuple2._2._1,stringTuple2Tuple2._2._2);
            }
        });
        JavaRDD<Row> StudentRowRDDS=  StudentRowRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                System.out.println("======================================="+row.toString());
                if(row.getInt(2)>80)
                    return true;

                return false;
            }
        });
        List<StructField> structFieldList=new ArrayList<StructField>();
        structFieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFieldList.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFieldList.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType=DataTypes.createStructType(structFieldList);
        Dataset studentRe=sqlContext.createDataFrame(StudentRowRDDS,structType);
        options.clear();
        options.put("url","jdbc:mysql://haha174:3306/test");
        options.put("dbtable","good_students_infos");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user","root");
        options.put("password","root");
   //     studentRe.write().format("jdbc").options(options).save();
        System.out.println("       ==================="+StudentRowRDDS.count());

        StudentRowRDDS.foreach(new VoidFunction<Row>() {
            private static final long serialVersionUID=1L;
            @Override
            public void call(Row o) throws Exception {

                Connection connection=null;
                Statement statement=null;
                try {
                    Class.forName("com.mysql.jdbc.driver");
                    connection= DriverManager.getConnection("jdbc:mysql://haha174:3306/test","root","root");
                    String sql="INSERT IN good_students_infos(name,age,score)values("+o.getString(0)+","+o.getInt(1)+","+o.getInt(2)+")";
                    System.out.println(sql);
                    statement=connection.createStatement();
                    statement.executeUpdate(sql);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                catch (SQLException e ){
                    e.printStackTrace();
                }finally {
                    if(connection!=null){
                        connection.close();
                    }
                    if(statement!=null){
                        statement.close();
                    }
                }

            }
        });
    }
}
