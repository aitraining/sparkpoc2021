package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase9_json {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase9_json").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    var df = spark.read
      .format("json")
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\durham-nc-foreclosure-2006-2016.json")
df.printSchema()
    df = df.withColumn("year", col("fields.year"))
      .withColumn("address", $"fields.address")
      .withColumn("geocode", explode($"fields.geocode"))
      .withColumn("parcel_number",$"fields.parcel_number")
      .withColumn("coordinates", explode(col("geometry.coordinates")))
      .withColumn("type",$"geometry.type")
      .drop("geometry","fields")

    // Shows at most 5 rows from the dataframe
    df.show(5)
    df.printSchema()


    spark.stop()
  }
}