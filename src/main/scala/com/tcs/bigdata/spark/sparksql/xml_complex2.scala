package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object xml_complex2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("xml_complex2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data ="C:\\work\\datasets\\ravi_kiran_trans00005.xml"
    val df=spark.read.format("xml").option("rowTag","Transaction").load(data).withColumnRenamed("_id","id")
    df.createOrReplaceTempView("tab")
    df.show()
    df.printSchema()
    spark.stop()
  }
}