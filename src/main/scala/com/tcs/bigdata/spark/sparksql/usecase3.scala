package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object usecase3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecases3").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val df = spark.read
      .format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\population_dept.csv.txt")
val cols = df.columns.map(x=>x.replaceAll("[^a-zA-Z0-9]",""))
    val df2 = df
      .withColumn("Code département",
        when(col("Code département")===("2A"), "20")
          .otherwise(col("Code département")))
      .withColumn("Code département",
        when(col("Code département").$eq$eq$eq("2B"), "20")
          .otherwise(col("Code département")))
      .withColumn("Code département",
        col("Code département").cast(DataTypes.IntegerType))
      .withColumn("Population municipale",
        regexp_replace(col("Population municipale"), ",", ""))
      .withColumn("Population municipale",
        col("Population municipale").cast(DataTypes.IntegerType))
      .withColumn("Population totale",
        regexp_replace(col("Population totale"), ",", ""))
      .withColumn("Population totale",
        col("Population totale").cast(DataTypes.IntegerType))
      .toDF(cols:_*).drop("c9")

    df2.show(25)
    df2.printSchema()

  /*  df2.write
      .format("delta")
      .mode("overwrite")
      .save("/tmp/delta_france_population")
*/
    println(s"${df2.count} rows updated.")

    spark.stop()
  }
}