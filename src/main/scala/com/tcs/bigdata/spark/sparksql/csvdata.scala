package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //load data
    // json ... u ll get from mobiles, social media, cloud, many servers gen json data

    val data = "C:\\work\\datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("delimiter",";").option("header","true").load(data)
    df.show()
    //process data
df.createOrReplaceTempView("tab") // run sql queries on top of dataframe
   // val res = spark.sql("select * from tab where balance>80000 and job='retired'")
 //   val res = spark.sql("select month, count(*) cnt from tab group by month order by cnt desc")
    //val res = df.where(col("balance")>50000 && col("job")==="retired")
    val res = df.groupBy($"month").count().orderBy($"count".desc)
    res.show()
    res.printSchema()

    spark.stop()
  }
}