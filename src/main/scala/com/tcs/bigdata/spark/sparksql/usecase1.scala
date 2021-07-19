package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecases").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data =""
    val df = spark.read.format("csv").option("header", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\books.csv.txt")


    val df1 = df.withColumn("main_author", split(col("authors"), "-").getItem(0))

    df1.show()

    // Shows at most 5 rows from the dataframe
    spark.stop()
  }
}