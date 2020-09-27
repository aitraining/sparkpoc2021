package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object minmax {
  def main(args: Array[String]) {
    def minmax(x:Int,y:Int) = {
      def max() = {
        if(x>y) s"max value $x"
        else s"min value $y"
      }
      def min() = {
        if(x>y) s"max value $x"
        else s"min value $y"
      }
      min();
      max();
    }
    println("Min and Max from 5, 7")
    minmax(5, 7);
  }
}