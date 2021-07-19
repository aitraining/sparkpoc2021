package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase11 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase11").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions
    var historicalScoresDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\WorldCupMatches.csv.txt")
    historicalScoresDf = historicalScoresDf.orderBy(historicalScoresDf.col("Year").desc).drop("Datetime").drop("Win conditions").drop("Half-time Home Goals").drop("Half-time Away Goals").drop("Referee").drop("Assistant 1").drop("Assistant 2").drop("RoundID").drop("MatchID").drop("Home Team Initials").drop("Away Team Initials").withColumnRenamed("Home Team Name", "country1").withColumnRenamed("Away Team Name", "country2").withColumnRenamed("Home Team Goals", "score1").withColumnRenamed("Away Team Goals", "score2")
    //log.debug("There were {} games in the history of the Soccer World Cup ", historicalScoresDf.count)
    historicalScoresDf.show(5)

    var lastWorldCupScoresDf= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\bigdata\\datasets\\nyc_school_attendance\\Cup.Russia.Matches.csv.txt")
    lastWorldCupScoresDf = lastWorldCupScoresDf.withColumnRenamed("Home Team", "country1").withColumnRenamed("Away Team", "country2").withColumnRenamed("Home Team Goals", "score1").withColumnRenamed("Away Team Goals", "score2").drop("Win Conditions").drop("Penalty").drop("Win").drop("Total Goals").drop("Datetime (Brazil)").withColumn("Year", functions.lit(2018))
    //log.debug("There were {} games at the Soccer World Cup 2018", lastWorldCupScoresDf.count)
    lastWorldCupScoresDf.show(5)

    val fullHistoricalScoresDf = historicalScoresDf.unionByName(lastWorldCupScoresDf)
    //log.debug("Up to now, there were {} games at the Soccer World Cups", fullHistoricalScoresDf.count)
    fullHistoricalScoresDf.show(5)

    val dfReverse = fullHistoricalScoresDf.withColumnRenamed("country1", "x").withColumnRenamed("country2", "country1").withColumnRenamed("x", "country2").withColumnRenamed("score1", "s").withColumnRenamed("score2", "score1").withColumnRenamed("s", "score2")

    val combinedDf= fullHistoricalScoresDf.unionByName(dfReverse).withColumnRenamed("country1", "country").drop("country2").withColumnRenamed("score1", "score").drop("score2")

    val franceScoreDf= combinedDf.filter("country='France'")
    //log.debug("France played {} games at Soccer World Cups", franceScoreDf.count)

    var mostPlayedDf = combinedDf.groupBy("country").count
    mostPlayedDf = mostPlayedDf.orderBy(mostPlayedDf.col("count").desc)
    mostPlayedDf.show(5)

    var goalsDf = combinedDf.groupBy("country").sum("score")
    goalsDf = goalsDf.orderBy(goalsDf.col("sum(score)").desc)
    goalsDf.show(5)

    System.out.println("Attendance per year")
    var attendanceDf = combinedDf.groupBy("Year").sum("Attendance")
    attendanceDf = attendanceDf.orderBy(attendanceDf.col("Year")).withColumnRenamed("sum(Attendance)", "Attendance")

    spark.stop()
  }
}