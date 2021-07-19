package com.tcs.bigdata.spark.sparksql
import java.sql.Date
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time._
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{functions => F}

object usecase7 {
  case class Book(authorId:Int, title:String, releaseDate:String, link:String, id:Int=0)
  /**
   * This is a mapper class that will convert a Row to an instance of Book.
   * You have full control over it - isn't it great that sometimes you have
   * control?
   *
   * @author rambabu.posa
   */
  def rowToBook(row: Row): Book = {
    val dateAsString = row.getAs[String]("releaseDate")

/*
    val releaseDate = LocalDate.parse(
      dateAsString,
      DateTimeFormatter.ofPattern("MM/dd/yy")
    )
*/

    Book(
      row.getAs[Int]("authorId"),
      row.getAs[String]("title"),
      row.getAs[String]("releaseDate") ,
      row.getAs[String]("link"),
      row.getAs[Int]("id"))
  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase7").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val filename = "D:\\bigdata\\datasets\\nyc_school_attendance\\books1.csv"
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    println("*** Books ingested in a dataframe")
    df.show(5)
    df.printSchema()

    import spark.implicits._
    val bookDs = df.map(rowToBook).as[Book]

    println("*** Books are now in a dataset of books")
    bookDs.show(5, 17)
    bookDs.printSchema()

    var df2 = bookDs.toDF

    df2 = df2.withColumn("releaseDateAsString",
      F.date_format(F.col("releaseDate"), "MM/dd/yy").as("MM/dd/yyyy"))

    println("*** Books are back in a dataframe")
    df2.show(5, 13)
    df2.printSchema()

    spark.stop()
  }
}