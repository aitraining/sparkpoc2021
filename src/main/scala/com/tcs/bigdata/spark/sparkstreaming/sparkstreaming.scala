package com.tcs.bigdata.spark.sparkstreaming

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object sparkstreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkstreaming").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //dstream
    val lines = ssc.socketTextStream("ec2-13-126-106-151.ap-south-1.compute.amazonaws.com", 1234)
    lines.foreachRDD { x =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
val prop = new Properties()
      prop.setProperty("user","msuername")
      prop.setProperty("password","mspassword")
      prop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

//val url ="jdbc:oracle:thin:@//myoracledb.conadtfguis7.ap-south-1.rds.amazonaws.com:1521/ORCL"
val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
      // Convert RDD[String] to DataFrame
      val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()
      df.createOrReplaceTempView("tab")
      val mas = spark.sql("select * from tab where city='mas'")
      mas.show()
     mas.write.mode(SaveMode.Append).jdbc(url,"de18tab",prop)
//   mas.write.mode(SaveMode.Overwrite).jdbc(ourl,"otherlive",oprop)
    }


    ssc.start() // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}