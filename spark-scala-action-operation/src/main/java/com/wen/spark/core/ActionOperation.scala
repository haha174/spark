package com.wen.spark

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {

 def main(args: Array[String]): Unit = {
  //reduce();
 //  collect();
 //  count();
  // take();
  // saveAsTextFile();
   //countByKey();
   fearch();
  }
 def reduce(){
   var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
   val sc=new SparkContext(conf);
   var numberArray=Array(1,2,3,4,5,6,7,8,9,10);
   var numbers=sc.parallelize(numberArray);
    var sum=numbers.reduce(_+_);
     println("sum            "+sum)

 }
  def collect(){
    var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
    val sc=new SparkContext(conf);
    var numberArray=Array(1,2,3,4,5,6,7,8,9,10);
    var numbers=sc.parallelize(numberArray);
    var doubleList=numbers.map(num=>num*2);
    var sumList=doubleList.collect();
    for (num <- sumList){
      println(num)
    }
  }
  def count(){
    var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
    val sc=new SparkContext(conf);
    var numberArray=Array(1,2,3,4,5,6,7,8,9,10);
    var numbers=sc.parallelize(numberArray);
    var num=numbers.count();
    println(num)
  }
  def take(){
    var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
    val sc=new SparkContext(conf);
    var numberArray=Array(1,2,3,4,5,6,7,8,9,10);
    var numbers=sc.parallelize(numberArray);
    var doubleList=numbers.map(num=>num*2);
    var sumList=doubleList.take(3);
    for (num <- sumList){
      println(num)
    }
  }
  def saveAsTextFile(){
    var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
    val sc=new SparkContext(conf);
    var numberArray=Array(1,2,3,4,5,6,7,8,9,10);
    var numbers=sc.parallelize(numberArray);
    var doubleList=numbers.map(num=>num*2);
    doubleList.saveAsTextFile("C://cwq//data//text.txt")
  }
  def countByKey(){
    var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
    val sc=new SparkContext(conf);
    var numberArray=Array(new Tuple2("class1",2),new Tuple2("class2",2),new Tuple2("class1",2),new Tuple2("class1",2));
   var RDDList=sc.parallelize(numberArray,1);
    var reduceResult=RDDList.countByKey();
    println(reduceResult)
  }
  def fearch(): Unit ={
    var conf=new SparkConf().setAppName("ActionOperation").setMaster("local");
    val sc=new SparkContext(conf);
    var numberArray=Array(
    new Tuple2[String, Tuple2[String, Integer]]("calss1", new Tuple2[String, Integer]("calss2", 1))
    , new Tuple2[String, Tuple2[String, Integer]]("calss3", new Tuple2[String, Integer]("calss4", 1))
    , new Tuple2[String, Tuple2[String, Integer]]("calss5", new Tuple2[String, Integer]("calss6", 1))
    , new Tuple2[String, Tuple2[String, Integer]]("calss7", new Tuple2[String, Integer]("calss8", 1))

    , new Tuple2[String, Tuple2[String, Integer]]("calss9", new Tuple2[String, Integer]("calss10", 1)))
    var RDDList=sc.parallelize(numberArray,1);
    RDDList.foreach(num=>{
      println(num._1+num._2._1+num._2._2);
    })
  }
}
