package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase16 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase16").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //# Location of the ratings.csv  file
      val RATINGS_CSV_LOCATION = "D:\\bigdata\\datasets\\nyc_school_attendance\\ml-latest-small\\ratings.csv"
  // val df = spark.read.csv(RATINGS_CSV_LOCATION)
    // # Loading CSV file with proper parsing and inferSchema
    val df1 = spark.read.format("csv").option("quote","\"").option("schema","userId INT, movieId INT, rating DOUBLE, timestamp INT").option("header","true").option("inferSchema","true").option("encoding","UTF-8").load(RATINGS_CSV_LOCATION)
     val df = df1.withColumnRenamed("timestamp", "timestamp_unix")
      .withColumn("timestamp", to_timestamp(from_unixtime($"timestamp_unix")))
   // # Displaying results of the load
    df.show()
    df.printSchema()
    df.describe().show()
    df.explain()
    val ratings=df1
    val moviesdata ="D:\\bigdata\\datasets\\nyc_school_attendance\\ml-latest-small\\movies.csv"
      val movies = spark.read.format("csv")
        .option("header","true")
        .option("quote","\"")
        .option("schema","movieId INT, title STRING, genres STRING").load(moviesdata)
    movies.show(15,true)
    movies.printSchema()

    movies.where(col("genres") === "Action").show(5, true)
    //movies.where($"genres" ==="Action").show(5, true)
    //movies.where("genres == 'Action'").show(5, true)
   val movie_genre = movies
        .withColumn("genres_array", split($"genres", "\\|"))
        .withColumn("genre", explode($"genres_array"))
        .select("movieId", "title", "genre")

   val available_genres = movie_genre.select("genre").distinct()
    available_genres.show()
    val movies_without_genre = movies.where(col("genres") === "(no genres listed)")
    print(movies_without_genre.count())
    movies_without_genre.show()

    spark.stop()
  }
}