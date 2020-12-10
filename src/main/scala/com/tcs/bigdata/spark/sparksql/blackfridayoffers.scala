package com.tcs.bigdata.spark.sparksql

import com.tcs.bigdata.spark.sparksql.allfunctions.{oprop, ourl}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

object blackfridayoffers {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("blackfridayoffers").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  //  val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val APIkey="qHl0FoziDxMVams3E1HuhfWqW"
    val APIsecretkey="gQ3DTEDNQ4vUH7rpiwNTuGEsxgvCHeOHD5KT4xdwgIAdLMCGO3"
    val Accesstoken ="181460431-uyxGu9euABiaNErFT1lRkIY5a084OGPODFQGqtt2"
    val Accesstokensecret="7HOxarANiJK0rQEPTzSYR3OSRRGQ8WsWaXJbfXwge6vcj"
//val searchwords =Seq("Black Friday, Thanksgiving")
    val searchwords = Seq("AI","Tensorflow","pytorch","artificial intelligence")

    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    val stream = TwitterUtils.createStream(ssc, None, searchwords)
//stream.print()

    stream.foreachRDD { rdd =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val df = rdd.map(x => (x.getUser.getScreenName(), x.getText(), x.getSource(), x.getPlace.getCountry(),x.getUser.getDescription())).toDF("person", "msg", "source","country","desc")
      //df.show(false)
    //  val res = df.where($"msg".like("%laptop%"))
      val res = df.where($"desc".contains("tensorflow","Artificial intelligence","pytorch"))
      res.show(false)
      res.write.jdbc(ourl,"laptopoffers",oprop)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}