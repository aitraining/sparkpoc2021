package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.slf4j.LoggerFactory
//com.tcs.bigdata.spark.sparksql.usecase13
object usecase13 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase13").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
    ))

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    val df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\populationbycountry19802010millions.csv")

    df.createOrReplaceTempView("geodata1")
    df.printSchema()

    val query =
      """
        |SELECT * FROM geodata1
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin

    val smallCountries = spark.sql(query)

    // Shows at most 10 rows from the dataframe (which is limited to 5
    // anyway)
    smallCountries.show(10, false)

    try
      Thread.sleep(6000) //wait 6 seconds to process
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }

    val schema1 = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1981", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1982", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1983", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1984", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1985", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1986", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1987", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1988", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1989", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1990", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1991", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1992", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1993", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1994", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1995", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1996", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1997", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1998", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1999", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2000", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2001", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2002", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2003", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2004", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2005", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2006", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2007", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2008", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2009", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2010", DataTypes.DoubleType, false)
    ))

    var df1 = spark.read.format("csv")
      .option("header", true)
      .schema(schema1)
      .load("D:\\bigdata\\datasets\\nyc_school_attendance\\populationbycountry19802010millions.csv")

    for(i <- Range(1981, 2010)) {
      df1 = df1.drop(df1.col("yr" + i))
    }

    // Creates a new column with the evolution of the population between
    // 1980
    // and 2010
    df1 = df1.withColumn("evolution", expr("round((yr2010 - yr1980) * 1000000)"))
    df1.createOrReplaceTempView("geodata")

  //  log.debug("Territories in orginal dataset: {}", df.count)
    val query1 =
      """
        |SELECT * FROM geodata
        |WHERE geo is not null and geo != 'Africa'
        | and geo != 'North America' and geo != 'World' and geo != 'Asia & Oceania'
        | and geo != 'Central & South America' and geo != 'Europe' and geo != 'Eurasia'
        | and geo != 'Middle East' order by yr2010 desc
      """.stripMargin

    val cleanedDf = spark.sql(query1)

    //log.debug("Territories in cleaned dataset: {}", cleanedDf.count)
    cleanedDf.show(20, false)
    //////////////////////////////////////





    spark.stop()
  }
}