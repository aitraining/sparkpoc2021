package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexcsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\10000Records.csv"
    val df = spark.read.format("csv").option("inferSchema","true").option("dateFormat","dd/MM/YYYY").option("delimiter",",").option("header","true").load(data)
    //df.createOrReplaceTempView("tab")
    df.show()
    df.printSchema()

    //DATA Cleaning
    //remove special characters from schema, header
    val reg = "[^a-zA-Z]"
    val cols = df.columns.map(x=>x.toLowerCase().replaceAll(reg,""))
val ndf = df.toDF(cols:_*)
    ndf.createOrReplaceTempView("tab")
  val res = spark.sql("select *,regexp_replace(email,'hotmail','hmail') emails from tab ").drop("email")

//  val res = ndf.withColumn("email", regexp_replace($"email","yahoo","yh"))
    //toDF() used to rename all columns ... _* represent if you have cols in the form of array use it
    res.show(false)
    res.printSchema()

    spark.stop()
  }
}