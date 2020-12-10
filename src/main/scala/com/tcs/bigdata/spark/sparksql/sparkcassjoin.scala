/*
package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.util._
object sparkcassjoin {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkcassjoin").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    
     spark.sql(
      """CREATE TABLE cassdb.asl123 USING cassandra PARTITIONED BY (city) AS SELECT * FROM cassdb.asl""")

    import spark.implicits._
    import spark.sql
    val edf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cassdb").option("table", "emp").load()
    val adf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cassdb").option("table", "asl").load()
    val join = edf.join(adf, $"first_name"===$"name").drop("name")
join.show()

    join.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace", "cassdb").option("table", "aslemp").save()
//create table aslemp(empid int primary key, deptid int, first_name varchar, last_name varchar, id int, city varchar);
    spark.stop()
  }
}
// if u integrate spark cassandra must use https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2 instead of latest connecter to solve compatability problems.
//python must be 2.7 not 3.*
*/
