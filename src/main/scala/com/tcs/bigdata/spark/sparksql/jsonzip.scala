package com.tcs.bigdata.spark.sparksql
import com.tcs.bigdata.spark.sparksql.allfunctions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object jsonzip {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsonzip").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
  df.createOrReplaceTempView("tab")
    //data cleaning
    val res = spark.sql("select cast(_id as int) id, city, loc[0] lang, loc[1] lati, pop, state from tab")
    res.createOrReplaceTempView("t1")
    val result = spark.sql("select state, count(*) cnt from t1 group by state order by cnt desc")
    result.show()
    res.write.jdbc(ourl,"ziptab",oprop)
    //res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace","venuks").option("table","zipjson").save()
    //res.write.format("hive").saveAsTable("jsontab")
    spark.stop()
  }
}