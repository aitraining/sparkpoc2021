package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvtesting {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvtesting").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val data = "D:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").load(data)
    //scala/java friendly process data
    //val res = df.select($"*").where($"state"==="NJ" && $"email".like("%yahoo%"))
    //val res = df.groupBy($"state").count().orderBy($"count".desc)
    val res = df.groupBy($"state").agg(count("*").as("cnt")).orderBy($"cnt".desc)


res.show()
    //df.show()
//    df.createOrReplaceTempView("tab")
    // allows to run sql query on top of dataframe
    //process data
    //val res = spark.sql("select * from tab where state='NJ' and email like '%yahoo%'")
    //val res = spark.sql("select state, count(*) cnt from tab group by state order by cnt desc")
    //res.show()

    spark.stop()
  }
}