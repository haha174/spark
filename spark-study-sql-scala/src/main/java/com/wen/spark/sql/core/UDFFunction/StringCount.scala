package com.wen.spark.sql.core.UDFFunction

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


class StringCount extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str",org.apache.spark.sql.types.StringType,true)))
  }
  //中间聚合时所处理的数据的类型
  override def bufferSchema: StructType={
    StructType(Array(StructField("count",org.apache.spark.sql.types.IntegerType,true)))
  }
  // 函数返回值的类型
  override def dataType: DataType = {
    org.apache.spark.sql.types.IntegerType
  }

  override def deterministic: Boolean = true
//为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0
  }
  //每个分组有一个新的值进来的时候如何进行分组对应的额聚合计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Int](0)+1
  }

  //由于spark  是分布式，所以一个分组的数组会在不同的节点上进行聚合，就是update
  //但是最后一个分组在各个节点上的聚合值进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }
  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
