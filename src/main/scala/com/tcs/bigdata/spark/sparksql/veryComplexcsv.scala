package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object veryComplexcsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("veryComplexcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\movies.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.show()
    val res = df.withColumn("moviename",expr("substring(title,1,length(title)-6)"))
      .withColumn("newYear",expr("substring(title,-5,4)"))
      .withColumn("newgenres",split($"genres","\\|")).select(col("*")+:(0 until 5).map(x =>col("newgenres").getItem(x).as(s"col$x")): _*)
        .drop("title","genres","moviename","newgenres")


    res.show(false)
    //find Comedy movies
    res.createOrReplaceTempView("tab")
    val result = spark.sql("select * from tab where  'Romance' in (col0, col1,col2,col3,col4)")
    //result.show()

    spark.stop()
  }
}