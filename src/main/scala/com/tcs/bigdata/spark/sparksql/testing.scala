package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object testing {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\bank\\bank-full.csv"
    val df = spark.read.format("csv").option("inferSchema","true").option("delimiter",";").option("header","true").load(data)
    df.show()
    df.printSchema()
    val df1 = df.groupBy("marital").count().orderBy($"count".desc)
    //df.where($"marital"==="married")
    df.where($"balance">=90000)
    df.where($"balance"!==80000)
    spark.stop()
  }
}