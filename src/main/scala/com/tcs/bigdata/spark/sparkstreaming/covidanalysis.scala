package com.tcs.bigdata.spark.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object covidanalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("covidanalysis").getOrCreate()
       val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val APIkey="Vpl3wEw078ctX8fEyrxWToepS"
    val APIsecretkey="tDU1i0kqMtieR7veb41UkstFWG0GSVPDzKAuqrTvaOgO1L8ycU"
    val Accesstoken="181460431-IPAq9EynnA4p7nZ84LhXNgXWlqCNX017V2bn3jUT"
    val Accesstokensecret ="DR6iuNIHLVmCZgggtMyKcAR4201uIyl8SDXO4Gy1CLHyB"


    ssc.start()
    ssc.awaitTermination()
  }
}