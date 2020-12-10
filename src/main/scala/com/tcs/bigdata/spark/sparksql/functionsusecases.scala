package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object functionsusecases {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("functionsusecases").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val df = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, -20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, -300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, -500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
    //df.show()
  //  df.printSchema()
//withColumn: if column exists--update data, if not exists create a new column
val ndf = df.withColumn("comm", abs($"comm"))
    //.withColumn("hiredate",regexp_replace($"hiredate","-","/"))
    //.withColumn("age", lit(18))
    //.withColumn("salgrade",when($"sal"<1300,"lowsal").when($"sal">=1300 and  $"sal"<2900,"avg sal").otherwise("highsal"))
    // by default add one value to a particular column

   // val gby = ndf.groupBy($"job").count()
   // val gby = ndf.groupBy($"job").agg(collect_list($"ename").alias("employees"))
  //  gby.show(false)

    //apply a function on top of all columns
    //val ndf = df.select(df.columns.map(x=>lower(col(x)).alias(x):_*))
   //   .withColumn("fullname", concat_ws(" ",$"ename",$"job",$"empno"))
    //.withColumn("fullname", concat($"ename", lit(" "),$"job", lit(" "),$"empno"))
   // .withColumn("sal",lpad($"sal",4,"*"))
    .withColumn("hiredate", to_date($"hiredate","dd-MMM-yy"))
   // .withColumn("today",current_date())
   // .withColumn("datediff",datediff($"today",$"hiredate"))
    // date diff : difference between two dates
    //.withColumn("after10days", date_add($"hiredate",10))
    //.withColumn("10daysback",date_sub($"hiredate",10))
    //.withColumn("date_truncd", date_trunc("day",$"hiredate"))
    .withColumn("date_truncm", to_date(date_trunc("month",$"hiredate")))
    .withColumn("date_truncy", to_date(date_trunc("year",$"hiredate")))
    .withColumn("datediff",datediff($"hiredate",$"date_truncm"))
    .withColumn("bonus", test($"job",$"sal"))
    
    .withColumn("finalsal",$"sal"+$"comm"+$"bonus")
    df.createOrReplaceTempView("tab")
    spark.udf.register("bonusoff",test)
    val res = spark.sql("select *, concat_ws(' ', ename, job) fullname, bonusoff(job, sal) fullsal from tab")
    res.show()
   // val test = spark.sql("")
//    ndf.show()
  //  ndf.printSchema()
    //df.printSchema()
    spark.stop()
    /*
    +---------+-----+
|  ANALYST|    1|SCOTT
| SALESMAN|    4|TURNER,MARTIN,WARD,ALLEN
|    CLERK|    2|
|  MANAGER|    3|
|PRESIDENT|    1|
+---------+-----+
     */
  }
  def bonus(job:String,sal:Int) = (job.toLowerCase,sal) match {
    case (j,s) if(j=="clerk" && s<1200) => s
    case (j,s) if(j=="salesman")=> s*80/100
    case (j,s) if(j=="manager")=> s*30/100
    case _ => 10*sal/100
  }
  val test = udf(bonus _)
}
