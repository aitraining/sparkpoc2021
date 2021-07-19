package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OrderStatisticsScalaApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("OrderStatisticsScalaApp").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data ="D:\\bigdata\\datasets\\orders.csv.txt"
    val df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(data)

    // Calculating the orders info using the dataframe API (DSL)
    val apiDf = df.groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("revenue"), avg("revenue"))

    apiDf.show(20)

    // Calculating the orders info using SparkSQL
    df.createOrReplaceTempView("orders")

    val sqlQuery = """SELECT firstName,lastName,state,SUM(quantity),SUM(revenue),AVG(revenue) FROM orders GROUP BY firstName, lastName, state"""

    val sqlDf = spark.sql(sqlQuery)
    sqlDf.show(20)
    spark.stop()
  }
}