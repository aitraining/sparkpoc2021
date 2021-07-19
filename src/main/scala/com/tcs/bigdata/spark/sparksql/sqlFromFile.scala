package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sqlFromFile {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sqlFromFile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val data = "D:\\bigdata\\datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load(data)
    df.createOrReplaceTempView("banktab")
    val file = "D:\\bigdata\\datasets\\queries.txt"
    for (x <- scala.io.Source.fromFile(file).getLines) {
val res = spark.sql(s"$x")
      res.show()
    }
    spark.stop()
  }
}