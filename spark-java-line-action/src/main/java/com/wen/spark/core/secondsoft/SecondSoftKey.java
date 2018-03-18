package com.wen.spark.core.secondsoft;

import org.apache.spark.SparkConf;
import java.io.Serializable;
import java.util.Objects;

import org.apache.spark.api.java.JavaSparkContext;
import scala.math.Ordered;
/**
 * 1.自定义二次排序的key  要实现 Ordered  和Serializable 接口 在key  中实现自己对多个列的排序算法。

  */
public class SecondSoftKey implements Ordered<SecondSoftKey> , Serializable {

    public SecondSoftKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $greater(SecondSoftKey that) {
        if(this.first>that.getFirst()){
            return true;
        }else if(this.first==that.getFirst()&&this.second>that.getSecond()){
           return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondSoftKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.getFirst()==that.getFirst()&&this.getSecond()==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(SecondSoftKey that) {
        if(this.first<that.getFirst()){
            return true;
        }else if(this.first==that.getFirst()&&this.second<that.getSecond()){
            return true;
        }
        return false;
    }
    @Override
    public boolean $less$eq(SecondSoftKey that) {
        if(this.$less(that)){
            return true;
        }else if(this.getFirst()==that.getFirst()&&this.getSecond()==that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compare(SecondSoftKey that) {
     if(this.getFirst()-that.getFirst()!=0){
         return this.getFirst()-that.getFirst();
     }else{
         return this.second-that.getSecond();
     }

    }

    @Override
    public int compareTo(SecondSoftKey that) {
        if(this.getFirst()-that.getFirst()!=0){
            return this.getFirst()-that.getFirst();
        }else{
            return this.second-that.getSecond();
        }
    }



    //  为进行排序的列提供set get equals  hashCode
    private int first;
    private int second;

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondSoftKey that = (SecondSoftKey) o;
        return first == that.first &&
                second == that.second;
    }

    @Override
    public int hashCode() {

        return Objects.hash(first, second);
    }
}
