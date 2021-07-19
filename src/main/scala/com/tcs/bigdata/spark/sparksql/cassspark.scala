package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object cassspark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("cassspark").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val adf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "venuks").option("table", "asl").load()
    val edf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "venuks").option("table", "emp").load()
adf.createOrReplaceTempView("a")
    edf.createOrReplaceTempView("e")
    val j = spark.sql("select e.*, a.city from e join a on e.last_name=a.name")
    j.show(4)
    j.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace", "venuks").option("table", "jointab").save()
   //create table jointab (empid int primary key, deptid int, first_name varchar, last_name varchar, city varchar);

    spark.stop()
  }
}