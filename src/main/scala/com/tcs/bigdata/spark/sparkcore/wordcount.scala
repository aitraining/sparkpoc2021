package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
//com.tcs.bigdata.spark.sparkcore.wordcount
object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.hadoop.validateOutputSpecs","false").appName("sparkrddpocs").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
  /*  val data = "C:\\work\\datasets\\wcdata.txt"
    val op = "C:\\work\\datasets\\output\\wcdataop"*/
    val data = args(0)
    val op = args(1)
    val rdd = sc.textFile(data)
    val skip = rdd.first()
val res = rdd.filter(x=>x!=skip).flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).sortBy(x=>x._2,false)
    res.take(9).foreach(println)
    res.coalesce(1).saveAsTextFile(op)

    spark.stop()
  }
}