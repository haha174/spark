package com.wen.spark.scala.SecondSoft

class SecondSoftKey(val first:Int,val second:Int) extends Ordered[SecondSoftKey] with Serializable {
  override def compare(that: SecondSoftKey): Int = {
    if(this.first-that.first!=0){
      return this.first-that.first
    }else {
      return this.second-that.second
    }
  }
}
