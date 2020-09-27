package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object jsoncomplex_history {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsoncomplex_history").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\history.json"
    val df=spark.read.format("json").load(data)
    df.show()
    df.printSchema()
    spark.stop()
  }
}