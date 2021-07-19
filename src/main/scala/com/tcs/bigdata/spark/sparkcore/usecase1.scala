package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price)
    val data = sc.textFile("D:\\bigdata\\datasets\\UserPurchaseHistory.csv")
      .map(line => line.split(","))
      .map(x => (x(0), x(1), x(2)))

    // let's count the number of purchases
    val numPurchases = data.count()

    // let's count how many unique users made purchases
    val uniqueUsers = data.map { case (user, product, price) => user }.distinct().count()

    // let's sum up our total revenue
    val totalRevenue = data.map { case (user, product, price) => price.toDouble }.sum()

    // let's find our most popular product
    val productsByPopularity = data
      .map { case (user, product, price) => (product, 1) }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)
    val mostPopular = productsByPopularity(0)

    // finally, print everything out
    println("Total purchases: " + numPurchases)
    println("Unique users: " + uniqueUsers)
    println("Total revenue: " + totalRevenue)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

    spark.stop()
  }
}