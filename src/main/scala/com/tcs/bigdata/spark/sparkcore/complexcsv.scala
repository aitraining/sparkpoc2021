package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession

object complexcsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
val data = "file:///C:\\work\\datasets\\10000Records.csv"
    val rdd = sc.textFile(data)
    val skip = rdd.first()
    val res = rdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6).replaceAll("yahoo","ya"),x(7),x(8),x(9),x(10),x(11)))
    res.take(20).foreach(println)
    spark.stop()
  }
}