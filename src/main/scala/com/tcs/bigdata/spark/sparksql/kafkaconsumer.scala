package com.tcs.bigdata.spark.sparksql

import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object kafkaconsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("SS_Kafka").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //10 seconds old data ... micro batch processing

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
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

    val topics = Array("logs")
    // this stream ... kafkautils ... get data from kafka brokers next create a Dstream basedon ssc, and topics and kakfa config
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //create dstream
    val lines = stream.map(x => x.value)
      lines.print()
    lines.foreachRDD { abc =>
      val spark = SparkSession.builder.config(abc.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      /*  val url ="jdbc:oracle:thin:@//bharathidb.c4nevuk0looq.us-east-2.rds.amazonaws.com:1521/ORCL"
      val prop = new  java.util.Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
      // Convert RDD[String] to DataFrame
      val df = abc.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.createOrReplaceTempView("tab")
      df.show()
      val mas = spark.sql("select * from tab where city='mas'")
      val del = spark.sql("select * from tab where city='del'")
      mas.write.mode(SaveMode.Append).jdbc(url,"masinfo",prop)
      del.write.mode(SaveMode.Append).jdbc(url,"delinfo",prop)
*/
    }
ssc.start()
    ssc.awaitTermination()
  }
}
