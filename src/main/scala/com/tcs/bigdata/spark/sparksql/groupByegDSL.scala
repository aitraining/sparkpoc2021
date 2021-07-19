package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object groupByegDSL {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("groupByegDSL").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val rawDf = spark.read
      .format("csv")
      .option("header", true)
      .option("sep", "|")
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\courses.csv.txt")

    // Shows at most 20 rows from the dataframe
    rawDf.show(20)

    // Performs the aggregation, grouping on columns id, batch_id, and
    // session_name
    val maxValuesDf = rawDf
      .select("*")
      .groupBy(col("id"), col("batch_id"), col("session_name"))
      .agg(max("time"))

    maxValuesDf.show(5)
    spark.stop()
  }
}