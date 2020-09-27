package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object importExportOracleData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("importExportOracleData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val ohost = "jdbc:oracle:thin://@oracle.cyvfcsfn6jja.ap-southeast-1.rds.amazonaws.com:1521/oracledb"
    val oprop = new java.util.Properties;
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver");
    oprop.setProperty("user", "ousername");
    oprop.setProperty("password", "opassword");
    oprop.setProperty("partitionColumn","DayofMonth")
    oprop.setProperty("lowerBound","10000")
    oprop.setProperty("upperBound","300000")
    oprop.setProperty("numPartitions","50")

    //val table = "(select empno,ename from emp e, salgrade s where e.sal between s.losal and s.hisal and to_char(hiredate,'mm')=grade) a"


    val odf = spark.read.jdbc(ohost,"EMP",oprop)
    /*
        val df = spark.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/home/hadoop/Documents/data/airline/2008.csv")
        println("started")
        df.write.jdbc(ohost,"airline",oprop)
        println("success")*/
    /*odf.createOrReplaceTempView("tab")
    val res = spark.sql("SELECT * FROM tab where sal>=3000")

    res.write.format("com.databricks.spark.csv").option("header","true").save("file:///home/hadoop/Documents/data/op/oracledata")
    res.write.format("com.databricks.spark.csv").option("header","true").save("hdfs://localhost:9000/oracledata")
    res.write.jdbc(ohost,"EMP_process",oprop)*/
    odf.show()
    spark.stop()
  }
}