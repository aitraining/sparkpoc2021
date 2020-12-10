package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object task {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("task").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\storesdataset.csv"
    val drdd = sc.textFile(data)
    val skip = drdd.first()
    val res = drdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(1),x(2).toInt)).reduceByKey((a,b)=>a+b)
    res.take(9).foreach(println)
    spark.stop()
  }
}