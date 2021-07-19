package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase5 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase5").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    var df = spark.read.format("json").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Restaurants_in_Durham_County_NC.json")

    println("*** Right after ingestion")
    df.show(5)
    df.printSchema()
    println("We have " + df.count + " records.")

    df = df.withColumn("county", lit("Durham"))
      .withColumn("datasetId", col("fields.id"))
      .withColumn("name", col("fields.premise_name"))
      .withColumn("address1", col("fields.premise_address1"))
      .withColumn("address2", col("fields.premise_address2"))
      .withColumn("city", col("fields.premise_city"))
      .withColumn("state", col("fields.premise_state"))
      .withColumn("zip", col("fields.premise_zip"))
      .withColumn("tel", col("fields.premise_phone"))
      .withColumn("dateStart", col("fields.opening_date"))
      .withColumn("dateEnd", col("fields.closing_date"))
      .withColumn("type", split(col("fields.type_description"), " - ").getItem(1))
      .withColumn("geoX", col("fields.geolocation").getItem(0))
      .withColumn("geoY", col("fields.geolocation").getItem(1))

    val cols_list = List(col("state"), lit("_"), col("county"), lit("_"), col("datasetId"))

    df = df.withColumn("id", concat(cols_list:_*))

    println("*** Dataframe transformed")
    df.show(5)
    df.printSchema()

    println("*** Looking at partitions")
    val partitionCount = df.rdd.getNumPartitions
    println("Partition count before repartition: " + partitionCount)

    df = df.repartition(4)
    println("Partition count after repartition: " + df.rdd.getNumPartitions)
    spark.stop()
  }
}