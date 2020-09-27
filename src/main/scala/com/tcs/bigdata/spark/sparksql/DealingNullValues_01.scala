package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
object DealingNullValues_01 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DealingNullValues_01").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    Logger.getLogger("org").setLevel(Level.ERROR)



    val customSchema = new StructType().
      add("name", StringType, true).
      add("company", StringType, false).
      add("salary", LongType, true)

    val employeeDF = spark.read.option("header","true").schema(customSchema).csv("C:\\work\\datasets\\employee_nulls.csv")
employeeDF.printSchema()
    employeeDF.show()
    // if null found values replace with specified values
    val result = employeeDF.na.replace("company",Map("TCS" -> "Tata Consultancy Service"))
    result.show()
    //if null values found fill with something
    val result1 = employeeDF.na.fill("noData",Seq("name","company"))
    result1.show()
    //DROP if any null values available in any column
    val result2 = employeeDF.na.drop()
    result2.show()

    spark.stop()
  }
}