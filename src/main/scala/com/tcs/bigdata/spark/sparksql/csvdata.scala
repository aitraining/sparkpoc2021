package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object csvdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.sql.shuffle.partitions",
    "1000").appName("sparkoraclepoc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //load data
    // json ... u ll get from mobiles, social media, cloud, many servers gen json data

    val data = "D:\\bigdata\\datasets\\bank-full.csv"
    // each block make 10mb
    val df = spark.read.format("csv").option("delimiter",";").option("header","true").option("inferSchema","true").load(data)
    df.show()
    df.rdd.getNumPartitions //5 part
    df.repartition(10) // before process use repartition . shuffle .. inc/dec num partition

    //process data
df.createOrReplaceTempView("tab") // run sql queries on top of dataframe
     val res = spark.sql("select * from tab where balance>80000")
 //   val res = spark.sql("select month, count(*) cnt from tab group by month order by cnt desc")
    //val res = df.where(col("balance")>50000 && col("job")==="retired")
   // val res = df.groupBy($"month").count().orderBy($"count".desc)
  //  val test = df.withColumn("agecat",when($"age".lt(50) && $"age".gt(30),"aged").when($"age".leq(30)&& $"age".gt(20),"youth").otherwise("oldaged"))
     // .withColumn("balance",lpad($"balance",5,"0"))
    res.show()
    //test.coalesce(1)
// no shuffle ... min partition s.. nt possible to increase partitions
  //  test.cache()
   /* test.explain()
    test.unpersist()

    test.persist(StorageLevel.MEMORY_AND_DISK)
    test.printSchema()*/

    spark.stop()
  }
}