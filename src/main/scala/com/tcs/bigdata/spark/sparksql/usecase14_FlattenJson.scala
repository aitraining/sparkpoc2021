package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase14_FlattenJson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase14_FlattenJson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val ARRAY_TYPE = "Array"
    val STRUCT_TYPE = "Struc"

    // Reads a JSON, stores it in a dataframe
    val invoicesDf = spark.read
      .format("json")
      .option("multiline", true)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\nested_array.json")
// clean dataframe
    val invoicesDf1 = invoicesDf.select($"author.*",$"books",$"publisher:*",$"data")
    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3)
    invoicesDf.printSchema()

    var recursion = false
    var processedDf = invoicesDf
    val schema = invoicesDf.schema
    val fields = schema.fields

    for (field <- fields) {
      field.dataType.toString.substring(0, 5) match {
        case ARRAY_TYPE =>
          // Explodes array
          processedDf = processedDf.withColumnRenamed(field.name, field.name + "_tmp")
          processedDf = processedDf.withColumn(field.name, explode(col(field.name + "_tmp")))
          processedDf = processedDf.drop(field.name + "_tmp")
          recursion = true

        case STRUCT_TYPE =>
          // Mapping
          /**
           * field.toDDL = `author` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `publisher` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `books` STRUCT<`salesByMonth`: ARRAY<BIGINT>, `title`: STRING>
           */
          println(s"field.toDDL = ${field.toDDL}")
          val ddl = field.toDDL.split("`") // fragile :(
          var i = 3
          while (i < ddl.length) {
            processedDf = processedDf.withColumn(field.name + "_" + ddl(i), col(field.name + "." + ddl(i)))
            i += 2
          }
          processedDf = processedDf.drop(field.name)
          recursion = true
        case _ =>
          processedDf = processedDf

      }
    }




    spark.stop()
  }
}