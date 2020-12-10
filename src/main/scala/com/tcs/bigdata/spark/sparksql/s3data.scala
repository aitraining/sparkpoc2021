package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
object s3data {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.hadoop.fs.s3a.access.key", "AKIAY36A2HGBENVUBKPN")
      .config("spark.hadoop.fs.s3a.secret.key", "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")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .appName("s3data").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key","AKIAY36A2HGBENVUBKPN")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key","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")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    import spark.implicits._
    import spark.sql
val data = "s3a://venukatragadda/zips.json"
    val df = spark.read.format("json").load(data)
    df.show()
    spark.stop()
  }
}