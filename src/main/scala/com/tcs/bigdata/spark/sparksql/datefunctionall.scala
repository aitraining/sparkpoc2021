package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datefunctionall {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.sql.session.timeZone", "IST").appName("datefunctionall").getOrCreate()
    //spark.sql.session.timeZon more info https://stackoverflow.com/questions/63498565/why-does-from-unixtime-in-spark-sql-return-a-different-date-time
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\mysqldata.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
  //  df.show()
    df.createOrReplaceTempView("tab")
    // convert timestamp/string to date format use to_date() its convert to spark understandable date format.
val res = df.select("dob","joindate").withColumn("dob",to_date($"dob", "yyyy-MM-dd"))
  .withColumn("joindate",to_date($"joindate","dd-MMM-yyyy"))
//  .withColumn("datediff", datediff($"joindate",$"dob")) //.orderBy($"datediff".desc)
  .withColumn("today",current_timestamp()) // today date you will get
 // .withColumn("monthdiff",months_between($"today",$"joindate"))
    //datediff used to find how many days difference between two days.
  //.withColumn("srexp",datediff($"today",$"joindate"))
  //.withColumn("add6month",add_months($"joindate",6)) // after 6 month prohibision  period completed.
  //.withColumn("nextSun", next_day(current_date(),"Sunday"))
  //.withColumn("truncatemonth",trunc($"joindate","month"))
  //.withColumn("truncateyear",trunc($"joindate","year"))
 // .withColumn("truncateday",date_trunc("day",$"joindate"))
 // .withColumn("truncatemonth",date_trunc("mon",$"joindate"))
 // .withColumn("truncateyr",date_trunc("year",$"joindate"))
  //.withColumn("qtr",quarter($"dob")) // 4 quarters, 1-3 q1, apr-jun 2, jul-sep 3, oct-dec 4
  //.withColumn("monthcol",month($"joindate"))
 // .withColumn("yearcol",year($"joindate"))
 // .withColumn("dayofmonth",dayofmonth($"joindate"))// this month 1 st how many days
 // .withColumn("dayofyear",dayofyear($"joindate")) // from jan-1 how many days
  .withColumn("dayofweek",dayofweek($"joindate")) //7 days ...1 means sun 7 means sat 2 monday
 // .withColumn("dayofweektest",date_format($"dayofweek",""))
  .withColumn("lastday",last_day($"joindate")) // every month last day you will get
  .withColumn("lastfri",next_day(date_sub($"lastday",7),"Friday"))
  .withColumn("unixts",unix_timestamp($"today")) // as of now how many sec completed
  .withColumn("temp",unix_timestamp(date_trunc("year",$"today"))) // at 2020-01-01 time how many seconds completed
  .withColumn("thisyrsec",$"unixts"-$"temp")
  .withColumn("sunmon",date_format($"joindate","EEEE")) // or use "E" to sun mon

    res.show()
    //val res = spark.sql("select * from tab")
    //res.show()
    res.printSchema()
    spark.stop()
  }
}