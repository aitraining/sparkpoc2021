package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.types._
//import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
//import spark.implicits._

//import org.apache.spark.metrics.source.{DoubleAccumulatorSource, LongAccumulatorSource}
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}
import org.apache.spark.sql.SparkSession

// Random number generator
import java.util.Random


import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
class customPartitioner ( override val  numPartitions: Int) extends Partitioner {
  override
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    return (k % numPartitions)
  }

  override
  def equals(other:scala.Any):Boolean = {
    other match {
      case obj: customPartitioner => obj.numPartitions == numPartitions
      case _  => false
    }
  }

}


class customPartitioner1 ( override
                           val numPartitions: Int) extends Partitioner {
  override
  def getPartition(Key:Any): Int =
    Key match {
      case s:String =>  { if ( s(0).toUpper == 'S' )  1 else 0 }

    }
  override
  def equals (other:Any):Boolean = {
    other.isInstanceOf[customPartitioner1]
  }
  override
  def hashCode: Int = 0
}
object customPartitioner {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("customPartitioner").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }

}