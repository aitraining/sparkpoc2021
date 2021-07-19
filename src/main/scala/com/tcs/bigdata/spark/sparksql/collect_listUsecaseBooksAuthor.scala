package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object collect_listUsecaseBooksAuthor {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("joinDataframeDSLBooksAuthor").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val filename: String = "D:\\bigdata\\datasets\\nyc_school_attendance\\authors.csv"
    val authorsDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    authorsDf.show
    authorsDf.printSchema

   val filename1 = "D:\\bigdata\\datasets\\nyc_school_attendance\\books.csv"
    val booksDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename1)


    booksDf.show
    booksDf.printSchema

    var libraryDf = authorsDf
      .join(booksDf, authorsDf.col("id") === booksDf.col("authorId"), "left")
      .withColumn("bookId", booksDf.col("id"))
      .drop(booksDf.col("id"))
      .groupBy(authorsDf.col("id"), authorsDf.col("name"), authorsDf.col("link"))
      .count

    //libraryDf = libraryDf.orderBy(libraryDf.col("count").desc)
    libraryDf = libraryDf.orderBy($"count".desc)

    libraryDf.show
    libraryDf.printSchema
/////////////////////////////////////////////
val libraryDf1 = authorsDf
  .join(booksDf, authorsDf.col("id") === booksDf.col("authorId"), "left")
  .withColumn("bookId", booksDf.col("id"))
  .drop(booksDf.col("id"))
  .orderBy(col("name").asc)

    libraryDf1.show()
    libraryDf1.printSchema()

    println("List of authors and their titles")
    var booksByAuthorDf = libraryDf1
      .groupBy(col("name"))
      .agg(collect_list("title"))

    booksByAuthorDf.show(false)
    booksByAuthorDf.printSchema()

    println("List of authors and their titles, with ids")
    booksByAuthorDf = libraryDf1
      .select("authorId", "name", "bookId", "title")
      .withColumn("book", struct(col("bookId"), col("title")))
      .drop("bookId", "title")
      .groupBy(col("authorId"), col("name"))
      .agg(collect_list("book").as("book"))

    booksByAuthorDf.show(false)
    booksByAuthorDf.printSchema()
    spark.stop()
  }
}