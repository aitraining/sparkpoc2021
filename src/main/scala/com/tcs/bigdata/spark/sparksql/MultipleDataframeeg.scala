package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.util.Properties

object MultipleDataframeeg {
  def main(args: Array[String]) {
    val t0: Long = System.currentTimeMillis

    val spark = SparkSession.builder.master("local[*]").appName("MultipleDataframeeg").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val t1 = System.currentTimeMillis

    var df = loadDataUsing2018Format(spark,
      Seq("D:\\bigdata\\datasets\\nyc_school_attendance\\2018*.csv.txt"))

    df = df.unionByName(loadDataUsing2015Format(spark,
      Seq("D:\\bigdata\\datasets\\nyc_school_attendance\\2015*.csv.txt")))

    df = df.unionByName(loadDataUsing2006Format(spark,
      Seq("D:\\bigdata\\datasets\\nyc_school_attendance\\200*.csv.txt", "D:\\bigdata\\datasets\\nyc_school_attendance\\2012*.csv.txt")))

    val t2 = System.currentTimeMillis

    val dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"

    // Properties to connect to the database,
    // the JDBC driver is part of our build.sbt
    /*val prop = new Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "password")
    df.write.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "ch13_nyc_schools", prop)
   */ val t3 = System.currentTimeMillis
    val log = LoggerFactory.getLogger("info")

    log.info(s"Dataset contains ${df.count} rows, processed in ${t3 - t0} ms.")
    log.info(s"Spark init ... ${t1 - t0} ms.")
    log.info(s"Ingestion .... ${t2 - t1} ms.")
    log.info(s"Output ....... ${t3 - t2} ms.")

    df.sample(.5).show(5)
    df.printSchema()
    spark.stop()
  }
  def loadDataUsing2018Format(spark: SparkSession, fileNames: Seq[String]): DataFrame = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("schoolId", DataTypes.StringType, false),
      DataTypes.createStructField("date", DataTypes.DateType, false),
      DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
      DataTypes.createStructField("present", DataTypes.IntegerType, false),
      DataTypes.createStructField("absent", DataTypes.IntegerType, false),
      DataTypes.createStructField("released", DataTypes.IntegerType, false)))

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("dateFormat", "yyyyMMdd")
      .schema(schema)
      .load(fileNames: _*)

    df.withColumn("schoolYear", lit(2018))
  }

  /**
   * Load a data file matching the 2006 format.
   *
   * @param fileNames
   * @return
   */
  private def loadDataUsing2006Format(spark: SparkSession, fileNames: Seq[String]): DataFrame =
    loadData(spark, fileNames, "yyyyMMdd")

  /**
   * Load a data file matching the 2015 format.
   *
   * @param fileNames
   * @return
   */
  private def loadDataUsing2015Format(spark: SparkSession, fileNames: Seq[String]): DataFrame =
    loadData(spark, fileNames, "MM/dd/yyyy")

  /**
   * Common loader for most datasets, accepts a date format as part of the
   * parameters.
   *
   * @param fileNames
   * @param dateFormat
   * @return
   */
   def loadData(spark: SparkSession, fileNames: Seq[String], dateFormat: String): DataFrame = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("schoolId", DataTypes.StringType, false),
      DataTypes.createStructField("date", DataTypes.DateType, false),
      DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
      DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
      DataTypes.createStructField("present", DataTypes.IntegerType, false),
      DataTypes.createStructField("absent", DataTypes.IntegerType, false),
      DataTypes.createStructField("released", DataTypes.IntegerType, false)))

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("dateFormat", dateFormat)
      .schema(schema)
      .load(fileNames: _*)

    df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4))
  }
}