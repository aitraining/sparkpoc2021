package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.tcs.bigdata.spark.sparksql.PointAttributionScalaUdaf
object PointsPerOrderScalaApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("PointsPerOrderScalaApp").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val pointsUdf = new PointAttributionScalaUdaf
    spark.udf.register("pointAttribution", pointsUdf)


    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\orders.csv.txt")

    // Calculating the points for each customer, not each order
    val pointDf = df
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), callUDF("pointAttribution", col("quantity")).as("point"))

    pointDf.show(20)

    // Alternate way: calculate order by order
    val max = 3
    val eachOrderDf = df
      .withColumn("point", when(col("quantity").$greater(max), max).otherwise(col("quantity")))
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("point").as("point"))

    eachOrderDf.show(20)
    spark.stop()
  }
}