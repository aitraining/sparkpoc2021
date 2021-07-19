package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase9_simplejson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase9_complexjson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    val df = spark.read
      .format("json")
      .option("multiline", true)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\countrytravelinfo.json")

    // Shows at most 3 rows from the dataframe
    df.show(3,false)

    df.printSchema()
    // Shows at most 3 rows from the dataframe
   // df.filter(df.col("_corrupt_record").isNotNull).count()

  //  df.show(3)
    df.printSchema
    spark.stop()
  }
}