package com.tcs.bigdata.spark.sparkstreaming

import com.tcs.bigdata.spark.sparksql.allfunctions.{oprop, ourl}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.functions._
object kafkaConsumernifi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("kafkaConsumer").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("nifi")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
// dstream created

   val lines=  stream.map(record =>  record.value)
    //lines.print()
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val reg = "(?P<ip>.*?) (?P<remote_log_name>.*?) (?P<userid>.*?) \[(?P<date>.*?)(?= ) (?P<timezone>.*?)\] \"(?P<request_method>.*?) (?P<path>.*?)(?P<request_version> HTTP/.*)?\" (?P<status>.*?) (?P<length>.*?) \"(?P<referrer>.*?)\" \"(?P<user_agent>.*?)\" (?P<session_id>.*?) (?P<generation_time_micro>.*?) (?P<virtual_host>.*)"
//val path = x.toString()
val df1 = spark.read.json(x)
      val df = df1.withColumn("t",explode($"results")).drop($"results")
        .select("t.user.picture.*","t.user.name.*","t.user.*", "t.user.location.*")
        .drop("picture","name","location")

      df.createOrReplaceTempView("tab")
      val url ="jdbc:oracle:thin:@//bharathidb.c4nevuk0looq.us-east-2.rds.amazonaws.com:1521/ORCL"
      val prop = new  java.util.Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
      df.write.mode(SaveMode.Overwrite).jdbc(url,"nifidata",prop)
     // val df = x.map(x => x.split(" ")).map(x => (x(0), x(3), x(4))).toDF("ip", "dt", "tz")

      // val df = spark.read.option("","true").json(x).withColumn("newcol",explode($"results")).drop($"results").select($"nationality",$"seed",$"newcol.user.*",$"newcol.user.location.*",$"newcol.user.name.*").drop("location","name","picture")
      //df.write.mode(SaveMode.Append).jdbc(ourl,"kafkalogs23dec",oprop)
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