package com.tcs.bigdata.spark.sparksql

import com.tcs.bigdata.spark.sparksql.allfunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.tcs.bigdata.spark.sparksql.OraMsSqlJoinTab
object OraMsSqlJoinTab {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("OraMsSqlJoinTab").getOrCreate()
    val spark = SparkSession.builder.config("spark.sql.session.timeZone","IST").master("local[*]").enableHiveSupport().appName("OraMsSqlJoinTab").getOrCreate()
    //to store in hive enablehiveSupport() is mandatory
    //in date function spark.sql.session.timeZone", "UTC" highly recommended.
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val odf = spark.read.jdbc(ourl,"EMP",oprop)
  //  odf.show()
    val msdf = spark.read.jdbc(msurl,"DEPT",msprop).withColumnRenamed("DEPTNO","MSDEPTNO")
  //  msdf.show()
    val join = odf.join(msdf,$"DEPTNO"===$"MSDEPTNO").drop(msdf("MSDEPTNO"))
    join.show()
    join.write.mode(SaveMode.Overwrite).format("hive").option("path","s3://santoshkumar2020/hive/warehousedir").saveAsTable("empdeptjointab")
    spark.stop()
  }
}