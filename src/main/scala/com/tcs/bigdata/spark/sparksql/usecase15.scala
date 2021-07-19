package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase15 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase15").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //# Set File Paths
    val tripdelaysFilePath = "D:\\bigdata\\datasets\\nyc_school_attendance\\departuredelays.csv"
    val airportsnaFilePath = "D:\\bigdata\\datasets\\nyc_school_attendance\\airport-codes-na.txt"

    //# Obtain airports dataset
    val airportsna = spark.read.format("csv").option("header","true").option("sep","\t").option("inferSchema","true").load(airportsnaFilePath)
    airportsna.createOrReplaceTempView("airports_na")
airportsna.printSchema()
    //# Obtain departure Delays data
      val departureDelays = spark.read.format("csv").option("header","true").option("inferSchema","true").load(tripdelaysFilePath)
    departureDelays.createOrReplaceTempView("departureDelays")
    departureDelays.cache()

//    # Available IATA codes from the departuredelays sample dataset
     val tripIATA = spark.sql("select distinct a.iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
    tripIATA.createOrReplaceTempView("tripIATA")
println("30line")
    airportsna.printSchema()
    airportsna.show(6)
    //# Only include airports with atleast one trip from the departureDelays dataset
    val airports = spark.sql("select f.iata, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
    airports.createOrReplaceTempView("airports")
    airports.cache()
    airports.show()

    //# Build `departureDelays_geo` DataFrame
    //#  Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)
    val departureDelays_geo = spark.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination")
    departureDelays_geo.show()
    spark.stop()
  }
}