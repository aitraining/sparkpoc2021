package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object databases {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("databases").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb"
    val mdf = spark.read.format("jdbc").option("url",url).option("user","msuername").option("password","mspassword").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable","DEPT").load()
    mdf.show()

    val ourl = "jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val odf = spark.read.format("jdbc").option("url",ourl).option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable","EMP").load()

    odf.show()
mdf.createOrReplaceTempView("tab1")
    odf.createOrReplaceTempView("tab2")
    val res = spark.sql("select m.*,d.loc,d.dname from tab1 m join tab2 d on m.deptno=d.deptno")
    res.show()
    res.write.format("jdbc").option("url",ourl).option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.driver.OracleDriver").option("dbtable","empdeptjoin").save()
    spark.stop()
  }
}