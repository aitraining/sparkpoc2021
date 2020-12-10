package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object window_functions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("window_functions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).withColumnRenamed("zip","sal")
    df.createOrReplaceTempView("tab")
    //val res = spark.sql("select * from tab where sal=(select max(sal) from tab)")
    //val res = spark.sql("select distinct(state) state, max(sal) maxsal from tab group by state")
    //val res = spark.sql("select state, sal, DENSE_RANK() over (partition by state order by sal desc) drank from tab").where($"drank"===1).drop($"drank")
    val win = Window.partitionBy($"state").orderBy($"sal".desc)
    //rank
    //val res =df.select($"*", rank().over(win).alias("rankcol"))
    //dense rank
     //val res = df.select($"*", dense_rank().over(win).alias("denrankcol"))
   // val res = df.select($"*", row_number().over(win).alias("rownumcol"))
    //val res = df.select($"state", $"sal", sum($"sal").over(win).alias("sumsalcol"))
    //val res = df.groupBy($"state").agg(sum($"sal").alias("sumsal")).orderBy($"sumsal".desc)
    //distinct(state),max(sal) group by state
    //val tres = df.select($"state", $"sal", percent_rank().over(win).alias("sumsalcol"))
    //val res = df.select($"state",$"sal", lead($"sal",1,0).over(win).alias("ledcol")).select($"state",$"sal"-$"leadcol".alias("diffsal"))
    val res = df.select($"state",$"sal", lag($"sal",1,0).over(win).alias("lagcol"))

    res.show()
    res.printSchema()
    spark.stop()
  }
}