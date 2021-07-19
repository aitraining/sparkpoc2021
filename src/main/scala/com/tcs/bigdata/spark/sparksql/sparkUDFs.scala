package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkUDFs {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkUDFs").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    def offer(pin:Int) = pin match {
      case x if(x>10000 && x<20000) => "30% off"
      case x if(x>20000 && x<40000)=> "20% off"
      case x if(x>=40000 && x<=50000) => "40% off"
      case x if(x>50000 && x<60000) => "50% off"
      case x if(x>=60000 && x<70000)=> "60% off"
      case x if(x>=70000 && x<=80000)=> "70% off"
      case x if(x>80000 && x<=90000)=> "80% off"
      case x if(x>90000 && x<= 100000)=> "90% off"
      case _ => "no offer"
    }

    //create udf means convert function to user define function
    val toff = udf(offer _) // _ it converts method to functions
spark.udf.register("off", toff) //call udf in spark sql ... u register like this.
val data = "D:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").load(data)
//    df.show(5)
    /*val res = df.withColumn("fullname", concat_ws(" ",$"first_name",$"last_name",$"state")).drop("first_name","last_name")
      .withColumn("phone1", regexp_replace($"phone1","-",""))
      .withColumn("phone2", regexp_replace($"phone2","-",""))*/
//    val res = df.groupBy($"state").agg(count($"first_name"))
    //val res = df.groupBy($"state").agg(count($"first_name").alias("cnt"),collect_list($"first_name").alias("listofnames")).orderBy($"cnt".desc)
    df.createOrReplaceTempView("tab")
    //val res = spark.sql("select state, count(*) cnt, collect_list(first_name) listofnames from tab group by state order by cnt desc")
 //   val res = df.withColumn("state", when($"state"==="NJ","NewJersey").when($"state"==="OH","ohio").otherwise($"state"))
   // val res = df.withColumn("email", regexp_replace($"email","gmail","googlemail"))
    //val res = df.withColumn("today offers", toff($"zip"))

   // val res = spark.sql("select *, off(zip) todayoffers from tab")
val res = df.withColumn("rno", monotonically_increasing_id()+1).where($"rno"===99).drop("rno")
    res.show(9)

    spark.stop()
  }
}