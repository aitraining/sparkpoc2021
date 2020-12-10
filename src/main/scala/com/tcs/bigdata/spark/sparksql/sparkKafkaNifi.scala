package com.tcs.bigdata.spark.sparksql

import com.tcs.bigdata.spark.sparksql.allfunctions.{oprop, ourl}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object sparkKafkaNifi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("sparkKafkaNifi").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_id_for_each_stream",
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
    lines.print()
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //  val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")

      val df = spark.read.json(x)
      val sch = df.schema


      val sch1= StructType(StructField("nationality",StringType,true) ::
        StructField("seed",StringType,true) ::
        StructField("version",StringType,true) ::
        StructField("AVS",StringType,true) ::
        StructField("BSN",StringType,true) ::
        StructField("PPS",StringType,true) ::
        StructField("TFN",StringType,true) ::
        StructField("cell",StringType,true) ::
        StructField("dob",DecimalType(19,0),true) ::
        StructField("email",StringType,true) ::
        StructField("gender",StringType,true) ::
        StructField("md5",StringType,true) ::
        StructField("password",StringType,true) ::
        StructField("phone",StringType,true) ::
        StructField("registered",DecimalType(19,0),true) ::
        StructField("salt",StringType,true) ::
        StructField("sha1",StringType,true) ::
        StructField("sha256",StringType,true) ::
        StructField("username",StringType,true) ::
        StructField("city",StringType,true) ::
        StructField("state",StringType,true) ::
        StructField("street",StringType,true) ::
        StructField("zip",DecimalType(19,0),true) :: Nil);
      val df1 = spark.read.schema(sch).json(x)

      //df.write.mode(SaveMode.Append).jdbc(ourl,"nifitab",oprop)
      //  df.show()
      //df.printSchema()

      val res = df.withColumn("r", explode($"results")).drop("results").select("*", "r.user.*", "r.user.location.*").drop("r", "picture", "name", "location")

      df1.createOrReplaceTempView("tab")
      /*val res = spark.sql("""select nationality, r.user.AVS, r.user.BSN, r.user.DNI,  r.user.INSEE, r.user.PPS, r.user.SSN, r.user.TFN, r.user.cell, r.user.dob,r.user.gender, r.user.location.city,
 r.user.location.state, r.user.location.street, r.user.location.zip, r.user.md5, concat_ws(' ',r.user.title,r.user.name.first, r.user.name.last) fullname, r.user.username, r.user.password, r.user.phone,
 r.user.picture.large,r.user.picture.thumbnail, r.user.registered, r.user.salt, r.user.sha1, r.user.sha256, seed, version from tab LATERAL view explode (results) t as r""")
     */ res.show()
      res.printSchema()
      //}
        res.write.mode(SaveMode.Append).jdbc(ourl, "nifikafkapoc", oprop)


    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()

  }
}