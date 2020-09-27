package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object exportToMssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("exportToMssql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\10000Records.csv"
    val df = spark.read.format("csv").option("delimiter",",").option("header","true").load(data)
    df.createOrReplaceTempView("tab")
    //DATA Cleaning
    //remove special characers from schema, header
    val reg = "[^a-zA-Z0-1]"
    val cols =  df.columns.map(x=>x.replaceAll(reg,"")).map(x=> x.toLowerCase)
    val ndf = df.toDF(cols:_*)

    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb"
    ndf.write.format("jdbc").option("url",url).option("user","msuername").option("password","mspassword").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable","export10kdata").save()

    spark.stop()
  }
}