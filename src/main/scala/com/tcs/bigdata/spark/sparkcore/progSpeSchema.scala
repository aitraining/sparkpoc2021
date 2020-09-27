package com.tcs.bigdata.spark.sparkcore
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object progSpeSchema {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("progSpeSchema").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\usdata.csv"
    val usrdd = sc.textFile(data)
    val header = usrdd.first()

    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val filter_RDD = usrdd.filter(x=>x!=header)
      .map(_.split(reg))
      .map(x => Row(x(0), x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))


    /*val fields = header.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val struct_schema = StructType(fields)
*/
    val struct_schema =
    StructType(Array(
      StructField("first_name1",StringType,true),
        StructField("last_name1",StringType,true),
        StructField("company_name1",StringType,true),
        StructField("address1",StringType,true),
        StructField("county1",StringType,true),
        StructField("state1",StringType,true),
        StructField("zip1",StringType,true),
        StructField("age1",StringType,true),
        StructField("phone11",StringType,true),
          StructField("phone21",StringType,true),
        StructField("email1",StringType,true),
        StructField("web1",StringType,true)));


    val df1 = spark.createDataFrame(filter_RDD,struct_schema)
        //val df1 = spark.read.format("csv").schema(struct_schema).option("header","true").load("filter_RDD")
    df1.createOrReplaceTempView("mydatatable")
    val structtable = spark.sql("select * from mydatatable")
    structtable.show()
    spark.stop()
  }
}