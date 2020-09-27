package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datetimezone {
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
    val data1 = "file:///C:\\work\\datasets\\mysqldatauszone.txt"
    val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1)
    //  df1.show()
    val dfclean = df.withColumn("joindate",from_utc_timestamp(from_unixtime(unix_timestamp(to_date($"joindate","dd-MMM-yyyy"))), "IST"))
    val df1clean = df1.withColumn("joindate",from_utc_timestamp(from_unixtime(unix_timestamp(to_date($"joindate","dd-MMM-yyyy"))), "US/Pacific"))
    dfclean.printSchema()
    dfclean.show(4)
    df1clean.printSchema()
    df1clean.show(5)

    val join = dfclean.join(df1clean,dfclean("empno")===df1clean("empno")).withColumn("test",datediff(df1clean("joindate"),dfclean("joindate")))
    join.show()



    spark.stop()
  }
}