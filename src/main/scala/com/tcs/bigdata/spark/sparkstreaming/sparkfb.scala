package com.tcs.bigdata.spark.sparkstreaming
//import com.github.catalystcode.fortis.spark.streaming.facebook.{FacebookAuth, FacebookUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object sparkfb {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfb").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
   // val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val FACEBOOK_APP_ID="321607914949550"
    val FACEBOOK_APP_SECRET="..."
    val FACEBOOK_AUTH_TOKEN="..."
    ssc.start()
  }
}