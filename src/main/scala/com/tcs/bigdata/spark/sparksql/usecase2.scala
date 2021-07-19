package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object usecase2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val df = spark.read.format("csv").option("header", true).option("sep", ";").option("enforceSchema", true)
      .option("nullValue", "null").option("inferSchema", true).load("D:\\bigdata\\datasets\\nyc_school_attendance\\BRAZIL_CITIES.csv.txt");


    df.show(10);
    df.printSchema();

    // Create and process the records without cache or checkpoint
    val df1 = df
      .orderBy(col("CAPITAL").desc)
      .withColumn("WAL-MART",
        when(col("WAL-MART").isNull, 0).otherwise(col("WAL-MART")))
      .withColumn("MAC",
        when(col("MAC").isNull, 0).otherwise(col("MAC")))
      .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
      .withColumn("GDP", col("GDP").cast("float"))
      .withColumn("area", regexp_replace(col("area"), ",", ""))
      .withColumn("area", col("area").cast("float"))
      .groupBy("STATE")
      .agg(
        first("CITY").alias("capital"),
        sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
        sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
        sum("POP_GDP").alias("pop_2016"),
        sum("GDP").alias("gdp_2016"),
        sum("POST_OFFICES").alias("post_offices_ct"),
        sum("WAL-MART").alias("wal_mart_ct"),
        sum("MAC").alias("mc_donalds_ct"),
        sum("Cars").alias("cars_ct"),
        sum("Motorcycles").alias("moto_ct"),
        sum("AREA").alias("area"),
        sum("IBGE_PLANTED_AREA").alias("agr_area"),
        sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
        sum("HOTELS").alias("hotels_ct"),
        sum("BEDS").alias("beds_ct"))
      .withColumn("agr_area", expr("agr_area / 100")) // converts hectares
      // to km2
      .orderBy(col("STATE"))
      .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"));
    df1.show()
    import org.apache.spark.sql.Dataset
    // Regions per population// Regions per population

//
    val popDf = df1.drop("area", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("pop_2016").desc)
    popDf.show(4)
    import org.apache.spark.sql.Dataset
    // Regions per size in km2

    val areaDf = df1.withColumn("area", round(col("area"), 2)).drop("pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("area").desc)
    import org.apache.spark.sql.Dataset
    // McDonald's per 1m inhabitants
    println("***** McDonald's restaurants per 1m inhabitants")
    val mcDonaldsPopDf = df1.withColumn("mcd_1m_inh", expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100")).drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("mcd_1m_inh").desc)
    mcDonaldsPopDf.show(5)
    import org.apache.spark.sql.Dataset
    // Walmart per 1m inhabitants
    println("***** Walmart supermarket per 1m inhabitants")
    val walmartPopDf = df1.withColumn("walmart_1m_inh", expr("int(wal_mart_ct / pop_2016 * 100000000) / 100")).drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("walmart_1m_inh").desc)
    walmartPopDf.show(5)

    import org.apache.spark.sql.Dataset
    // GDP per capita// GDP per capita

    println("***** GDP per capita")
    val gdpPerCapitaDf = df1.drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area").withColumn("gdp_capita", expr("int(gdp_capita)")).orderBy(col("gdp_capita").desc)
gdpPerCapitaDf.show(5)

    import org.apache.spark.sql.Dataset
    // Post offices// Post offices

    println("***** Post offices")
    val postOfficeDf = df1.withColumn("post_office_1m_inh", expr("int(post_offices_ct / pop_2016 * 100000000) / 100")).withColumn("post_office_100k_km2", expr("int(post_offices_ct / area * 10000000) / 100")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil").orderBy(col("post_office_1m_inh").desc)

    import org.apache.spark.sql.Dataset
    println("****  Per 1 million inhabitants")
    val postOfficePopDf = postOfficeDf.drop("post_office_100k_km2", "area").orderBy(col("post_office_1m_inh").desc)
    postOfficePopDf.show(5)
    println("****  per 100000 km2")
    val postOfficeArea = postOfficeDf.drop("post_office_1m_inh", "pop_2016").orderBy(col("post_office_100k_km2").desc)

    postOfficeArea.show(7)
    import org.apache.spark.sql.Dataset
    // Cars and motorcycles per 1k habitants
    // Cars and motorcycles per 1k habitants

    println("***** Vehicles")
    val vehiclesDf = df1.withColumn("veh_1k_inh", expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area", "pop_brazil").orderBy(col("veh_1k_inh").desc)

    import org.apache.spark.sql.Dataset

    // Cars and motorcycles per 1k habitants
    println("***** Agriculture - usage of land for agriculture")
    val agricultureDf = df1.withColumn("agr_area_pct", expr("int(agr_area / area * 1000) / 10")).withColumn("area", expr("int(area)")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod", "pop_2016").orderBy(col("agr_area_pct").desc)
    agricultureDf.show(6)
    spark.stop()
  }
}