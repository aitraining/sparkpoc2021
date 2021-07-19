package com.tcs.bigdata.spark.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.kafka.clients.producer._
//import kafka.serializer.StringDecoder

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
object kafkaProducer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaProducer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //val path = args(0)
  //  val path = "C:\\bigdata\\logs\\access_log_20210630-083049.log"
    val path = args(0)
    val logrdd = sc.textFile(path)
    val topic = "logs"
    logrdd.foreachPartition(abc => {
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")


      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      abc.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //
        //(indpak, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)

      })

    })

    ssc.start()
    ssc.awaitTermination()
  }
}