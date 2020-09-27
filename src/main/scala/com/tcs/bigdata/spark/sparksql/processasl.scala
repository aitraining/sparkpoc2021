package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object processasl {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("processasl").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\asldata.txt"
    val df = spark.read.format("csv").option("header","false").load(data).toDF("name","age","city")
    df.createOrReplaceTempView("tab")
    spark.udf.register("venu",udf(ageoffer _))
    val result = spark.sql("select *, venu(age) todayoffer from tab")
    result.show(false)

    spark.stop()
  }


  def ageoffer(myage:Int) = myage match {
    case myage if(myage>=1 && myage<13) => "J&J product 30% off"
    case myage if(myage>=13 && myage<=22) =>"mobiles 30% off"
    case myage if(myage>18 && myage<30) => "30% off on laptops"
    case myage if(myage>=30 && myage<60) => "20% off on insurance payment"
    case x if(x>60 && x<100) => "30% off on health products"
    case _ => "no offers"

  }
}