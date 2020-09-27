package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compjsonusc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("compjsonusc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\spark-2.4.5-bin-hadoop2.7\\python\\test_support\\sql\\people_array_utf16le.json"
    val df = spark.read.format("json").load(data)
    df.printSchema()
    df.show()
    spark.stop()
  }
}