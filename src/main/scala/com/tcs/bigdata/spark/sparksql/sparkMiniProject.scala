package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.tcs.bigdata.spark.sparksql.allfunctions._
object sparkMiniProject {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkMiniProject").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

   val odf = spark.read.format("jdbc").option("url",ourl).option("dbtable","EMP").option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.OracleDriver").load()
    val msdf = spark.read.format("jdbc").option("url",msurl).option("dbtable","DEPT").option("user","msuername").option("password","mspassword").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    odf.createOrReplaceTempView("e")
    msdf.createOrReplaceTempView("d")
    val res = spark.sql("select e.*,d.loc, d.dname from e join d on e.deptno=d.deptno")
    res.show()
    spark.stop()
  }
}

// sqoop .... data import/export .. just u r getting data from oracle store in hdfs/hive
// spark ... import, processing, get live data, batch data, ml ... atleast 2 thing  use spark
