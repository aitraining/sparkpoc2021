package com.tcs.bigdata.spark.sparksql

import org.apache.kafka.clients.producer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
/*
this code main purpose, something server/app generate logs, that logs store in one file with interval .
get that logs using kafka producer api or any other language ..
 */
import scala.io.Source
object kafkaproducer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkKafkaProducer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    import scala.util.Try
    //write any code (java, scala or spark) to get logs from path file
    //val path = args(0)
    val path = "D:\\bigdata\\pyspark\\access_log_20210203-234157.log"
    val data = spark.sparkContext.textFile(path)
    // val data = Source.fromFile(path).getLines.toList
    data.foreachPartition(rdd => {
      import java.util._

      val props = new java.util.Properties()
      props.put("metadata.broker.list", "localhost:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")

      // import kafka.producer._
      // val config = new ProducerConfig(props)
      //producer code to send data to kafka servers
      val producer = new KafkaProducer[String, String](props)
      val topic = "logs".toSeq
      rdd.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indeng,"anu,56,mas")
        Thread.sleep(5000)
        //kafka send million msg per seconds. 2 logs 10 sec ... all logs send within fraction of milli seconds. so its not visible so
        //thats y if u mention Thread.sleep(5000) wait 5000 milli seconds (5 sec) next send logs


      })

    })


    ssc.start()
    ssc.awaitTermination()
  }

}
