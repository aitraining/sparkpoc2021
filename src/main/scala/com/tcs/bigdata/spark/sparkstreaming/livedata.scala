package com.tcs.bigdata.spark.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import com.tcs.bigdata.spark.sparksql.allfunctions._

object livedata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("livedata").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("ec2-13-233-27-96.ap-south-1.compute.amazonaws.com", 1234)
    //lines.print()
   // val res = lines.flatMap(x=>x.split(",")).map(x=>(x,1)).reduceByKey(_ + _)
    val res = lines.foreachRDD { x=>
     val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
     import spark.implicits._

val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
     df.createOrReplaceTempView("tab")
     val del = spark.sql("select * from tab where city='del'")
     val blr =  spark.sql("select * from tab where city='blr'")
     val mas =  spark.sql("select * from tab where city='mas'")
     //val other =  spark.sql("select * from tab where city not in ('mas','blr','del')")
/*del.write.mode(SaveMode.Append).jdbc(ourl,"delhilive",oprop)
     blr.write.mode(SaveMode.Append).jdbc(ourl,"blrlive",oprop)
     mas.write.mode(SaveMode.Append).jdbc(ourl,"maslive",oprop)*/
     //other.write.mode(SaveMode.Overwrite).jdbc(ourl,"otherlive",oprop)
     del.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "del", "keyspace" -> "venuks")).save()
     mas.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "mas", "keyspace" -> "venuks")).save()
     blr.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "blr", "keyspace" -> "venuks")).save()
     //other.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "blr", "keyspace" -> "venuks")).save()
     df.show()
   }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}