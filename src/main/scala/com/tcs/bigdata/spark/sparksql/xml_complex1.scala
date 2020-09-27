package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object xml_complex1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.memory.fraction","0.75").appName("xml_complex1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\xmldata.xml"
    val df=spark.read.format("xml").option("rowTag","course").load(data).withColumnRenamed("_id","id")
    df.createOrReplaceTempView("tab")
    val df1 = df.select("*","place.*","time.*").drop("place","time")
     // .withColumn("end_time", to_date($"end_time"))
     // .withColumn("start_date",($"start_time"))
    df1.show()
    df1.printSchema()
//    df.show()
    df.printSchema()
    spark.stop()
  }
}