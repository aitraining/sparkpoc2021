package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object wcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("wcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\wcdata.txt"
    val rdd = sc.textFile(data)
    val af = (a:Int, b:Int)=> a+b

    val res = rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(af).sortBy(x=>x._2,false)
//any "bykey" method/function process only tuple info
//val res = rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).groupByKey().map(x=>(x._1,x._2.sum)).sortBy(x=>x._2,false)
    res.collect.foreach(println)
    spark.stop()
  }
}
// venu,30
//def reduceByKey(func: (V, V) => V): RDD[(K, V)]
//(the,1) // (2,1) = > 2+1=3....(3,1) => 3+1=4.. (the,4)
//(the,1)
//(the,1)
//(the,1)
// (1,1) => 2
// (the,2)

//data transfer from one container to another container to do further processing
//called shufeling
/*
mmmmmmmmm.ffffffffffffffffffffff.fmfmfmfmfmfmfmfmfm.... 1stg
each map 100mb... 100m..10gb mem
100fm......10gb
100f.....10gb

spark allocate memory based on stages..... stage1 ...100mb, stage2: 150mb
if data shuf... wide transformation..data is shuffeled from one server to another server to do further computation
if no data shuff...narrow...data processed within same server... narrow trans

 */
