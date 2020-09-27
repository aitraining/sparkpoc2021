package com.tcs.bigdata.spark.sparkstreaming

import com.tcs.bigdata.spark.sparksql.allfunctions.{oprop, ourl}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object kafkaConsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("kafkaConsumer").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("xyz")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
// dstream created
   val lines=  stream.map(record =>  record.value)
    lines.print()
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
    //  val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")

      val df = spark.read.option("","true").json(x).withColumn("newcol",explode($"results")).drop($"results").select($"nationality",$"seed",$"newcol.user.*",$"newcol.user.location.*",$"newcol.user.name.*").drop("location","name","picture")
df.write.mode(SaveMode.Append).jdbc(ourl,"nifitab",oprop)
      df.show()
      df.printSchema()
      df.createOrReplaceTempView("tab")
//spark.sql("show tables;")
 //df.write.mode(SaveMode.Append).saveAsTable("hiv")



     /* df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where city='mas'")
      val res1 = spark.sql("select * from tab where city='del'")
      res.write.mode(SaveMode.Append).jdbc(ourl,"masinfo",oprop)
      res1.write.mode(SaveMode.Append).jdbc(ourl,"delhiinfo",oprop)
*/
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()

  }
}