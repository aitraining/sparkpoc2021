package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase10 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase10").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions
    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    var worldCupRussiaMatchesDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Cup.Russia.Matches.csv.txt")
    worldCupRussiaMatchesDf = worldCupRussiaMatchesDf.withColumnRenamed("Home Team", "country1").withColumnRenamed("Away Team", "country2").withColumnRenamed("Home Team Goals", "score1").withColumnRenamed("Away Team Goals", "score2").withColumn("date", functions.split(worldCupRussiaMatchesDf.col("Datetime (Brazil)"), ".-.").getItem(1)).drop("Datetime (Brazil)")

    //log.debug("There were {} games in the Soccer World Cup 2018", worldCupRussiaMatchesDf.count)
    worldCupRussiaMatchesDf.show(5)

    val worldCupRussiaTeamsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Cup.Russia.Teams.csv.txt")
    worldCupRussiaTeamsDf.show(5)

    val fixtureDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Fixture.csv.txt")
    fixtureDf.show(5)

    val playersScoreDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Players_Score.csv.txt")
    playersScoreDf.show(5)

    val playersStatsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Players_Stats.csv.txt")
    playersStatsDf.show(5)

    val playersDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Players.csv.txt")
    playersDf.show(5)

    val teamsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Teams.csv.txt")
    teamsDf.show(5)

    val worldCupMatchesDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\WorldCupMatches.csv.txt")
    worldCupMatchesDf.show(5)

    val worldCupPlayersDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\WorldCupPlayers.csv.txt")
    worldCupPlayersDf.show(5)

    val worldCupsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\WorldCups.csv.txt")
    worldCupsDf.show(5)

    val dfReverse = worldCupRussiaMatchesDf.withColumnRenamed("country1", "x").withColumnRenamed("country2", "country1").withColumnRenamed("x", "country2").withColumnRenamed("score1", "s").withColumnRenamed("score2", "score1").withColumnRenamed("s", "score2")
    dfReverse.show(5)

    val combinedDf = worldCupRussiaMatchesDf.unionByName(dfReverse).withColumnRenamed("country1", "country").drop("country2").withColumnRenamed("score1", "score").drop("score2")
    combinedDf.show(5)
   // log.debug("There were {} interactions in the Soccer World Cup 2018", combinedDf.count)

    val franceScoreDf = combinedDf.filter("country='France'")
    franceScoreDf.show()

    var mostPlayedDf = combinedDf.groupBy("country").count
    mostPlayedDf = mostPlayedDf.orderBy(mostPlayedDf.col("count").desc)
    mostPlayedDf.show(5)

    var goalsDf = combinedDf.groupBy("country").sum("score")
    goalsDf = goalsDf.orderBy(goalsDf.col("sum(score)").desc)
    goalsDf.show(5)

    println("And the winner is...")
    try Thread.sleep(1434)
    catch {
      case e: InterruptedException =>

      // oh well, let's ignore it
    }

    println("France")
    spark.stop()
  }
}