package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark._
import sun.security.util.Length

object cutpartition {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("cutpartition").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val arr = Array("venu", "anu", "sudha", "pavithra", "raja", "roja", "pakija")
    val rdd: RDD[String] = sc.parallelize(List("hadoop", "idea", "name", "hadoop", "spark", "map", "map", "age", "student", "123456"))

    val rd2: RDD[(String, Int)] = rdd.map((_, 1))
    val op = "C:\\work\\datasets\\output\\part"
    rd2.reduceByKey(_ + _).partitionBy(new CustomPartition(arr.length)).saveAsTextFile(op)

    println("number of partitions: " + rd2.getNumPartitions)
    spark.stop()
  }

  class CustomPartition(val num: Int) extends Partitioner {

    override def numPartitions: Int = {
      num
    }

    override def getPartition(key: Any): Int = {
      val length: Int = key.toString.length
      length match {
        case 4 => 0
        case 5 => 1
        case 6 => 2
        case _ => 0
      }
    }
  }

}