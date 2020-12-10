package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
object sparkxml {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkxml").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\books.xml"
    val df = spark.read.format("xml").option("rowTag","book").load(data)
   // df.show()
    df.printSchema()
    //dsl
val res = df.groupBy($"author").count().orderBy($"count".desc)
    res.show()
    res.printSchema()
    spark.stop()
  }
}