package com.tcs.bigdata.spark.sparksql
import com.tcs.bigdata.spark.sparksql.allfunctions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkfunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("delimiter",",").option("header","true").load(data)
    df.createOrReplaceTempView("tab")
    // withColumn used to create new column if column doesn't exist. update if column exists
    //concat_ws concatenate two or more columns with given seperator ( space _ - etc)
    // if u mention "first_name" spark get confuse is it string or column. if u mention $ spark consider as column.
    //val res = df.withColumn("fullname",concat_ws("_",col("first_name"),col("last_name"),$"city"))
   // val res = spark.sql("select *, concat_ws(' ', first_name, last_name, city) fullname from tab")

    /*val res = df.withColumn("phone1", regexp_replace($"phone1","-",""))
        .withColumn("phone2", regexp_replace($"phone2","-",""))
        .withColumn("state", regexp_replace($"state","OH","Ohio"))
    res.show()*/
    /*val res = spark.sql("select first_name, last_name, company_name, address, city, county, regexp_replace(state,'OH','Ohio') state, zip, regexp_replace(phone1, '-','') phone1, regexp_replace(phone2, '-','') phone2, email, web from tab")
    res.show()*/
    //val res = spark.sql("select * from tab")
    /*val res = df.withColumn("line",monotonically_increasing_id()+1)
        .where($"line"===10).orderBy($"zip".desc).drop("line")
    res.show()
    */
   /* val res = df.rdd.take(10).last
    print(res)*/
    // dataframe convert to rdd with df.rdd ... rdd(row) ..
val res = df.withColumn("getemail", substring($"email",1,5))
        .withColumn("subind",substring_index($"email","@",-1))
    //array("venu","gamail")

    /*val result = res.groupBy($"subind").count().orderBy($"count".desc)
    result.show()*/
    // in this email column after @
    // here count must be 0,1 or -1 ... minus 1 means last value 1 means first value
//    res.show(false)
    /*val res1 = spark.sql("select substr(email,instr(email,'@')+1) email, count(*) cnt from tab group by substr(email,instr(email,'@')+1) order by cnt desc")
    res1.show()*/

    //cust func

    val conatfunc = udf(conc _)
    val res1 = df.withColumn("fullname", conatfunc($"first_name",$"last_name") )
    //by default spark don't know other functions/methods, use udf to convert to spark understandable format
    res1.show()
    spark.udf.register("myfun",conatfunc)
    val res2 = spark.sql("select *, myfun(first_name,last_name) fullname from tab")
    res2.show()

    val myoff = udf(stateoffers _)
    spark.udf.register("myoffer", myoff)

    val res3=spark.sql("select *, myoffer(state) steteoffers from tab")
res3.show()
    spark.stop()
  }

}
