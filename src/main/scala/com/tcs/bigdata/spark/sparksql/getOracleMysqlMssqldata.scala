package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object getOracleMysqlMssqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOracleMysqlMssqldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val murl = "jdbc:mysql://mysqldb.cchcz22yzyo4.ap-southeast-1.rds.amazonaws.com:3306/mysqldb"
    val mtable = "emp"
    val mprop = new java.util.Properties
    mprop.setProperty("driver", "com.mysql.jdbc.Driver");
    mprop.setProperty("user", "musername")
    mprop.setProperty("password", "mpassword");

    val employee = spark.read.jdbc(murl, mtable, mprop)

    employee.show();
    val ourl = "jdbc:oracle:thin://@oracle.cchcz22yzyo4.ap-southeast-1.rds.amazonaws.com:1521/ORACLEDB"
    val otable = "dept"
    val oprop = new java.util.Properties
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver");
    oprop.setProperty("user", "ousername")
    oprop.setProperty("password", "opassword");
    val dept = spark.read.jdbc(ourl, otable, oprop)
    dept.show();
    employee.createOrReplaceTempView("mysqltab")
    dept.createOrReplaceTempView("oracletab")
    employee.cache()
    dept.cache()

    val result = spark.sql("select m.ename, m.sal, o.loc from mysqltab m join oracletab o on (m.deptno=o.deptno)")
    result.show()
   // result.write.format("com.databricks.spark.csv").option("header","true").save("file:////home/hadoop/Desktop/result/mysqloracledata")
//store data in oracle again
    result.write.jdbc(ourl,"result",oprop)
    spark.stop()
  }
}