package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datefunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datefunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\11augcoronacases.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
  //  df.show()
    //clean dataset and schema
    val reg = "[^a-zA-Z]"
    val cols = df.columns.map(x=>x.toLowerCase.replaceAll(reg,""))
    val ndf = df.toDF(cols:_*)
  //  ndf.show()
    val df1 = ndf.withColumn("samplesentdate",regexp_replace($"samplesentdate","\\.","-"))
      .withColumn("samplesentdate",to_date($"samplesentdate","dd-MM-yyyy"))
      .withColumn("sampleresultdate",to_date($"sampleresultdate","dd.MM.yyyy"))
      .withColumn("sampleresultdate",to_date($"sampleresultdate","dd-MM-yyyy"))
      .withColumn("sampleresultdate",to_date($"sampleresultdate","dd/MM/yyyy"))
    //to_date used convert string/timestamp to date format. spark default date format yyyy-MM-dd ..
      .withColumn("sex",when($"sex"==="FEMALE","F").when($"sex"==="MALE","M").otherwise(upper($"sex")))
      .withColumn("sno",lpad($"sno",3,"0"))
      .withColumn("test", when($"test".isNull,"Others").otherwise($"test"))

  //  df1.show()
   // df1.printSchema()
    //date functions
    //add_months
    val df2 = df1.withColumn("add6mon",add_months($"samplesentdate",6))
     //after 6 months whats date you will get
     //.withColumn("sub6mon",add_months($"samplesentdate",-6))
     //six month before whats date
     //  .withColumn("add21days",date_add($"samplesentdate",21))
     //after 20 days whats date you will get
    // .withColumn("sub21days",date_add($"samplesentdate",-21))
     //20 days before whats date you will get
    //   .withColumn("currdate",current_date())
    //   .withColumn("currTS",current_timestamp())
    //   .withColumn("samplesentdateTS",to_timestamp($"samplesentdate"))
       .withColumn("daysToCompleteTest",datediff($"sampleresultdate",$"samplesentdate"))
       .withColumn("nextMon",next_day(current_date(),"Monday"))
       .withColumn("datetruncate",date_add(date_trunc("month",current_date()),27))

       //.withColumn("howmaydays",datediff(current_date(),$"datetruncate"))


    //whats today date you will get
df2.show(false)
    //val gb = df2.groupBy($"test").count().orderBy($"count".desc)
    //val gb = df2.groupBy($"test").agg(avg($"daysToCompleteTest")).as("avgdays")

   // gb.show()

    spark.stop()
  }
} // 9700655144 srikanth....mani 7401535415... prabhu 9884641396