package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase4 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase3").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    // Reads a CSV file with header, called
    // Restaurants_in_Wake_County_NC.csv,
    // stores it in a dataframe
    var df = spark.read.format("csv").option("header", "true")
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\Restaurants_in_Wake_County_NC.csv.txt")
    println("*** Right after ingestion")

    df.show(5)
    df.printSchema()
    println("We have " + df.count + " records.")

    // Let's transform our dataframe
    df =  df.withColumn("county", lit("Wake"))
      .withColumnRenamed("HSISID", "datasetId")
      .withColumnRenamed("NAME", "name")
      .withColumnRenamed("ADDRESS1", "address1")
      .withColumnRenamed("ADDRESS2", "address2")
      .withColumnRenamed("CITY", "city")
      .withColumnRenamed("STATE", "state")
      .withColumnRenamed("POSTALCODE", "zip")
      .withColumnRenamed("PHONENUMBER", "tel")
      .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
      .withColumnRenamed("FACILITYTYPE", "type")
      .withColumnRenamed("X", "geoX")
      .withColumnRenamed("Y", "geoY")
      .drop("OBJECTID","PERMITID","GEOCODESTATUS")

    val df1 = df.withColumn("id",
      concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")))

    // Shows at most 5 rows from the dataframe
    println("*** Dataframe transformed")
    df1.show(5)

    // for book only
    val drop_cols=List("address2","zip","tel","dateStart",
      "geoX","geoY","address1","datasetId")
    val dfUsedForBook = df1.drop(drop_cols:_*)
    dfUsedForBook.show(5, 15)
    // end

    df = df.withColumn("id",
      concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")))


    println("*** Looking at partitions")
    val partitions = df.rdd.partitions
    val partitionCount = partitions.length
    println("Partition count before repartition: " + partitionCount)

    df = df.repartition(4)
    println("Partition count after repartition: " + df.rdd.partitions.length)
    spark.stop()

    // NEW
    ////////////////////////////////////////////////////////////////////
    val schema = df.schema

    println("*** Schema as a tree:")
    schema.printTreeString()
    val schemaAsString = schema.mkString
    println("*** Schema as string: " + schemaAsString)
    val schemaAsJson = schema.prettyJson
    println("*** Schema as JSON: " + schemaAsJson)

    df.printSchema()

  }
}