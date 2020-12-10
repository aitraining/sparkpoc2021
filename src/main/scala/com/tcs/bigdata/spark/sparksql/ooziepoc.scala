package com.tcs.bigdata.spark.sparksql
import com.tcs.bigdata.spark.sparksql.allfunctions._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ooziepoc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ooziepoc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val tab = args(0)
val df= spark.read.jdbc(ourl,tab,oprop).toDF("deptno", "dname", "loc")
    val cols = df.columns.map(x=>x.toLowerCase)
    val ndf = df.toDF(cols:_*)
    //df.write.format("hive").saveAsTable(tab)
    ndf.show()
df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","dept").option("keyspace","venudb").save()
    df.printSchema()
    spark.stop()
  }
}