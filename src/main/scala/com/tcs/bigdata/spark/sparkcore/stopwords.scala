package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object stopwords {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("stopwords").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val input =args(0)
    val stpworld =args(1)
    val stopwords = scala.io.Source.fromFile(stpworld).getLines().toSet

    val res = spark.sparkContext.textFile(input)
      .flatMap(line => line.split("\\W+"))
      .map(_.toLowerCase)
      .filter( s => !stopwords.contains(s))
      .filter( s => s.length > 2)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    res.take(9).foreach(println)
    spark.stop()
  }
}