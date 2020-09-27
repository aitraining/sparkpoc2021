package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object getallMysqlOracleTables {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("getallMysqlTables").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //val spark = SparkSession.builder.master("local[*]").appName("getallMysqlTables").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
   // val spark = SparkSession.builder.master("local[*]").appName("getallMysqlTables").getOrCreate()
   // val sc = spark.sparkContext
    //val conf = new SparkConf().setAppName("getallMysqlTables").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val output=args(0)
    val query = "(SELECT * FROM information_schema.tables where TABLE_SCHEMA='MysqlDB' ) tables"
    val url = "jdbc:mysql://mysqltcsproject.ckzjezatnmoy.ap-south-1.rds.amazonaws.com:3306/MysqlDB"
    val prop = new java.util.Properties()
    prop.setProperty("user", "musername")
    prop.setProperty("password", "mpassword")
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    //above three prop use to get data from oracle ... but not optimized
    //below properties used to optimize to get data from oracle/any other database
    prop.setProperty("partitionColumn","flightnum")
    prop.setProperty("lowerBound","0")
    prop.setProperty("upperBound","1000000")  //per batch 1000 records available
    prop.setProperty("numPartitions","1000")
    prop.setProperty("sessionInitStatement","delete from india where records=null")
    //.... 100 cr rec in oracle
    prop.setProperty("fetchsize","1000") // each thread get 1000 rows instead of 10 rows.
    prop.setProperty("batchsize","1000")

    //not optimize



    val df = spark.read.jdbc(url, query, prop)
    df.show(9)
    val alltab  = df.select("table_name").rdd.map(r => r(0)).collect.toList
    // alltab.foreach(println)

    alltab.foreach{x=>
      val tab = x.toString()
      println(s"printing $x table ")
      val res= spark.read.jdbc(url,s"$x",prop)

      res.write.format("csv").option("header","true").save(output)
      res.show()
    }
    /*
    val query = "(select table_name from all_tables where TABLESPACE_NAME='USERS') tmptst"
    val url = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop = new java.util.Properties()
    prop.setProperty("user", "ousername")
    prop.setProperty("password", "opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    val df = spark.read.jdbc(url, query, prop)
  //  df.show()
    val alltab  = df.select("TABLE_NAME").rdd.map(r => r(0)).collect.toList

//val tables = Array("EMP","DEPT","BONUS")
    alltab.foreach{x=>
      val table = x.toString()
  //  val query = "(select * from emp where sal > 2000) tmp"
    val df1 = spark.read.jdbc(url, table , prop)
    df1.show()
//      df1.write.format("orc").saveAsTable(table)
    }
     */
    spark.stop()
  }
}