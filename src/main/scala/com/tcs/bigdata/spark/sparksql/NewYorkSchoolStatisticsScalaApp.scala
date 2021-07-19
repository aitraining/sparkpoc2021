package com.tcs.bigdata.spark.sparksql
import com.tcs.bigdata.spark.sparksql.NewYorkSchoolStatisticsScalaApp.loadData
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.slf4j.LoggerFactory
/*
few spark developers everything submitting in the form of functions. You can reuse functions properly
 */
object NewYorkSchoolStatisticsScalaApp {
  def loadDataUsing2018Format(spark:SparkSession, fileNames: String*):DataFrame = {
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
      .load(fileNames:_*)

    df.withColumn("schoolYear", F.lit(2018))
  }


  def loadDataUsing2006Format(spark:SparkSession, fileNames: String*) =
    loadData(spark,fileNames, "yyyyMMdd")

  /**
   * Load a data file matching the 2015 format.
   *
   * @param fileNames
   * @return
   */
  def loadDataUsing2015Format(spark:SparkSession, fileNames: String*): DataFrame =
    loadData(spark, fileNames, "MM/dd/yyyy")

  /**
   * Common loader for most datasets, accepts a date format as part of the
   * parameters.
   *
   * @param fileNames
   * @param dateFormat
   * @return
   */
  def loadData(spark:SparkSession, fileNames: Seq[String], dateFormat: String): DataFrame = {
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
      .load(fileNames:_*)

    df.withColumn("schoolYear", F.substring(F.col("schoolYear"), 1, 4))

  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("NewYorkSchoolStatisticsScalaApp").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
     val log = LoggerFactory.getLogger("Info")


    var masterDf = loadDataUsing2018Format(spark,"D:\\bigdata\\datasets\\nyc_school_attendance\\2018*.csv")

    masterDf = masterDf.unionByName(loadDataUsing2015Format(spark,"D:\\bigdata\\datasets\\nyc_school_attendance\\2015*.csv"))

    masterDf = masterDf.unionByName(loadDataUsing2006Format(spark,
      "D:\\bigdata\\datasets\\nyc_school_attendance\\200*.csv", "D:\\bigdata\\datasets\\nyc_school_attendance\\2012*.csv"))
    masterDf = masterDf.cache

    // Shows at most 5 rows from the dataframe - this is the dataframe we
    // can use to build our aggregations on
    log.debug("Dataset contains {} rows", masterDf.count)
    masterDf.sample(.5).show(5)
    masterDf.printSchema()

    // Unique schools
    val uniqueSchoolsDf = masterDf.select("schoolId").distinct
    log.debug("Dataset contains {} unique schools", uniqueSchoolsDf.count)

    // Calculating the average enrollment for each school
    val averageEnrollmentDf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .avg("enrolled", "present", "absent")
      .orderBy("schoolId", "schoolYear")

    log.info("Average enrollment for each school")
    averageEnrollmentDf.show(20)

    // Evolution of # of students in the schools
    val studentCountPerYearDf = averageEnrollmentDf
      .withColumnRenamed("avg(enrolled)", "enrolled")
      .groupBy(F.col("schoolYear"))
      .agg(F.sum("enrolled").as("enrolled"))
      .withColumn("enrolled", F.floor("enrolled").cast(DataTypes.LongType))
      .orderBy("schoolYear")

    log.info("Evolution of # of students per year")
    studentCountPerYearDf.show(20)

    val maxStudentRow = studentCountPerYearDf
      .orderBy(F.col("enrolled").desc)
      .first

    val year = maxStudentRow.getString(0)
    val max = maxStudentRow.getLong(1)

    log.debug(s"${year} was the year with most students, the district served ${max} students.")

    // Evolution of # of students in the schools
    val relativeStudentCountPerYearDf = studentCountPerYearDf
      .withColumn("max", F.lit(max))
      .withColumn("delta", F.expr("max - enrolled"))
      .drop("max")
      .orderBy("schoolYear")

    log.info(s"Variation on the enrollment from ${year}:" )
    relativeStudentCountPerYearDf.show(20)

    // Most enrolled per school for each year
    val maxEnrolledPerSchooldf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .max("enrolled")
      .orderBy("schoolId", "schoolYear")

    log.info("Maximum enrollement per school and year")
    maxEnrolledPerSchooldf.show(20)

    // Min absent per school for each year
    val minAbsenteeDf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .min("absent")
      .orderBy("schoolId", "schoolYear")

    log.info("Minimum absenteeism per school and year")
    minAbsenteeDf.show(20)

    // Min absent per school for each year, as a % of enrolled
    var absenteeRatioDf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .agg(F.max("enrolled").alias("enrolled"), F.avg("absent").as("absent"))

    absenteeRatioDf = absenteeRatioDf
      .groupBy(F.col("schoolId"))
      .agg(F.avg("enrolled").as("avg_enrolled"), F.avg("absent").as("avg_absent"))
      .withColumn("%", F.expr("avg_absent / avg_enrolled * 100"))
      .filter(F.col("avg_enrolled").gt(F.lit(10)))
      .orderBy("%", "avg_enrolled")

    log.info("Schools with the least absenteeism")
    absenteeRatioDf.show(5)

    log.info("Schools with the most absenteeism")
    absenteeRatioDf.orderBy(F.col("%").desc).show(5)


  }
}