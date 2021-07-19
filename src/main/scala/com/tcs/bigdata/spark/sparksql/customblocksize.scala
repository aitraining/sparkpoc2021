package com.tcs.bigdata.spark.sparksql

import org.apache.hadoop.fs.Path._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration


import org.apache.hadoop.fs._
object customblocksize {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("customblocksize").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //val data ="C:\\work\\datasets\\output\\Firesmallfiles\\200mb"
    val data = args(0)
    val op = args(1)
    //val op ="C:\\work\\datasets\\output\\Fire200smallfiles\\mergescalafiles"
    val fs = FileSystem.get(sc.hadoopConfiguration)
//    val if u want to rename, delete use it fs.delete(new Path(path), true)
//fs.rename(new Path(newPath), new Path(path))
val path = new Path(data)

    val dirSize = fs.getContentSummary(path).getLength
    println("folder size"+dirSize)
    val fileNum = dirSize/(128 * 1024 * 1024.0)
    val df = spark.read.format("csv").option("header","true").load(data)
    df.coalesce(fileNum.ceil.toInt).write.format("csv").save(op)
    spark.stop()
  }
}