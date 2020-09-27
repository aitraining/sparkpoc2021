package com.tcs.bigdata.spark.sparkstreaming

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.tcs.bigdata.spark.sparksql.allfunctions._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object sparkStreamingWH {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkStreamingWH").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    // wait 10 sec .. how much u got set as micro batch ... send to spark core to process.
   // val sc = spark.sparkContext
// create dstream... in realtime env ... create dstream .. headache process

   val lines = ssc.socketTextStream("15.207.86.58", 1235)
    //socket stream get data from terminal and consume from port

    //lines.print()
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
     df.show()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where city='mas'")
      val res1 = spark.sql("select * from tab where city='del'")
      res.write.mode(SaveMode.Append).jdbc(ourl,"masinfo",oprop)
      res1.write.mode(SaveMode.Append).jdbc(ourl,"delhiinfo",oprop)

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()

  }
}