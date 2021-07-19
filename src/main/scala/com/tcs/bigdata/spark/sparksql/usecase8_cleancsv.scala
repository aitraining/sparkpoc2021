package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase8_cleancsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase8_cleancsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("quote", "*")
      .option("dateFormat", "MM/dd/yyyy")
      .option("inferSchema", true)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\books2.csv")
      .withColumn("releaseDate", to_date($"releaseDate","MM/dd/yyyy"))

    println("Excerpt of the dataframe content:")

    // Shows at most 7 rows from the dataframe, with columns as wide as 90
    // characters
    df.show(7, 70)
    println("Dataframe's schema:")
    df.printSchema()
    spark.stop()
  }
}