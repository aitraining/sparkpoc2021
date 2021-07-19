package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase6 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase6").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    var df1 = spark.read.format("csv").option("header", "true")
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\Restaurants_in_Wake_County_NC.csv.txt")

    var df2 = spark.read.format("json")
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\Restaurants_in_Durham_County_NC.json")




    val drop_cols = List("OBJECTID", "GEOCODESTATUS", "PERMITID")
    var df3 = df1.withColumn("county", lit("Wake"))
      .withColumnRenamed("HSISID", "datasetId")
      .withColumnRenamed("NAME", "name")
      .withColumnRenamed("ADDRESS1", "address1")
      .withColumnRenamed("ADDRESS2", "address2")
      .withColumnRenamed("CITY", "city")
      .withColumnRenamed("STATE", "state")
      .withColumnRenamed("POSTALCODE", "zip")
      .withColumnRenamed("PHONENUMBER", "tel")
      .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
      .withColumn("dateEnd", lit(null))
      .withColumnRenamed("FACILITYTYPE", "type")
      .withColumnRenamed("X", "geoX")
      .withColumnRenamed("Y", "geoY")
      .drop(drop_cols:_*)

    df3 = df3.withColumn("id",
      concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
    // I left the following line if you want to play with repartitioning
    // df1 = df1.repartition(4);

    val drop_col=List("fields", "geometry", "record_timestamp", "recordid")
    var df4 = df2.withColumn("county", lit("Durham"))
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
      .drop(drop_col:_*)

    df4 = df4.withColumn("id",
      concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
    // I left the following line if you want to play with repartitioning
    // df1 = df1.repartition(4);

    val res = df3.unionByName(df4)
    res.show(5)
    res.printSchema()
    println("We have " + res.count + " records.")
    val partitionCount1 = res.rdd.getNumPartitions
    println("Partition count: " + partitionCount1)

    spark.stop()
  }
}