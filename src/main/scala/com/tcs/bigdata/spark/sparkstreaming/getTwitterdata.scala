package com.tcs.bigdata.spark.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

object getTwitterdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getTwitterdata").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
   // val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val APIkey= "35Oxiv2LBALn9t5ta6X16cHh9"
    val APIsecretkey= "0et7fwtGaTysQTphpCKFqJRXNWzQVXDMarsZ8KM19ulndGlNDP"
    val Accesstoken = "181460431-IFG9vfrX8nUkwrPhq25seHjzcOxy2CJ4M9J8UCXw"
    val Accesstokensecret ="o2QckYZQ683AdRJZ1WS48wr3o6xn3dEjwNqtNSxQlNrov"

    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    //val lines = ssc.socketTextStream("localhost", 9999)

    val searchFilter = "trump,CORONA, USA election, Election2020, DONALD TRUMP "
    // create dstream
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))


    tweetStream.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //  val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      ///val df = x.map(x=>)
      val df = x.map(x => (x.getText(), x.getUser().getScreenName(),x.getCreatedAt().getTime())).toDF("msg", "username","createdDate")

      // val df = spark.read.json(x).withColumn("newcol",explode($"results")).drop($"results").select($"nationality",$"seed",$"newcol.user.",$"newcol.user.location.",$"newcol.user.name.*").drop("location","name","picture")
      //      df.write.mode(SaveMode.Append).jdbc(ourl,"nifitab",oprop)
    //  df.show(false)
      df.printSchema()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where msg like '%https://%'")
    res.show(false)
      val path = "file:///C:\\work\\datasets\\output\\twitterdata"
      res.write.format("csv").option("header","true").save(path)
      res.write.format("org.apache.spark.sql.cassandra").option("keyspace","venuks").option("table","twitter").save()

/*      val ourl ="jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oprop = new java.util.Properties()
      oprop.setProperty("user","ousername")
      oprop.setProperty("password","opassword")
      oprop.setProperty("driver","oracle.jdbc.OracleDriver")
      res.write.mode(SaveMode.Append).jdbc(ourl,"tweets",oprop)*/
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}