package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexjsonurl {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexjsonurl").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\jsondata\\luftdaten_data.json"
    val df = spark.read.format("json").load(data)
    //df.show()

    val res = df.withColumn("sdv",explode($"sensordatavalues"))
      .select($"id",$"location.*",$"sampling_rate",$"sensor.*",
        $"sensor.sensor_type.*",$"timestamp", $"sdv.id".as("sdvid"),
        $"sdv.value".as("sdvvalue"),$"sdv.value_type".as("sdvvalue"))
      .drop($"sensor_type")
        res.show()
    df.printSchema()
    res.printSchema()
    spark.stop()
  }
}