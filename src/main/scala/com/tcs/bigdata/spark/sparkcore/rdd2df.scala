package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object rdd2df {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rdd2df").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
//Row.fromSeq
val data = "D:\\bigdata\\datasets\\bank-full.csv"
    val brdd = sc.textFile(data)
    val skip = brdd.first()
    val odata = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","").split(";")).map(x=>Row.fromSeq(x))
    val cols = skip.replaceAll("\"","").split(";")
      .map(x => StructField(x, StringType, nullable = true))
    val schema = StructType(cols)

    val df = spark.createDataFrame(odata,schema)

    df.show(5)

    spark.stop()
  }
}