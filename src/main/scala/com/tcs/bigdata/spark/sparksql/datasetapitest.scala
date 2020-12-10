package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datasetapitest {
  case class testcc(name:String, age:Int)
  case class gbscc(state:String, cnt:Long)
  case class aslcc(first_name:String, last_name:String, company_name:String, address:String, city:String, county:String, state:String, zip:Int, phone1:String, phone2:String, email:String, web:String)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datasetapitest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext //to create rdd
    val sqlContext = spark.sqlContext // to create dataframe
   // val data = "C:\\work\\datasets\\us-500.csv"
    import spark.implicits._
    
    // if u r convert rdd to dataframe or dataframe to dataset ..
    val data = "file:///C:\\work\\datasets\\jsondata\\world_bank\\world_bank.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    val ds = df.as[maincc]
    ds.createOrReplaceTempView("tab")

  /*  val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

    import spark.implicits._
    import spark.sql
val ds = df.as[aslcc]
ds.createOrReplaceTempView("tab")
    //val res = spark.sql("select * from tab where state='NY'").as[aslcc]
    val res = spark.sql("select state, count(*) cnt from tab group by state order by cnt desc")
     val res1 = res.as[gbscc]
    res1.show()*/

    spark.stop()
  }
  case class id(
                  `$oid`: String
                )
  case class Majorsector_percent(
                                  Name: String,
                                  Percent: Double
                                )
  case class Mjsector_namecode(
                                name: String,
                                code: String
                              )
  case class Project_abstract(
                               cdata: String
                             )
  case class Projectdocs(
                          DocTypeDesc: String,
                          DocType: String,
                          EntityID: String,
                          DocURL: String,
                          DocDate: String
                        )
  case class Sector(
                     Name: String
                   )
  case class maincc(
                     _id: id,
                     approvalfy: Double,
                     board_approval_month: String,
                     boardapprovaldate: String,
                     borrower: String,
                     closingdate: String,
                     country_namecode: String,
                     countrycode: String,
                     countryname: String,
                     countryshortname: String,
                     docty: String,
                     envassesmentcategorycode: String,
                     grantamt: Double,
                     ibrdcommamt: Double,
                     id: String,
                     idacommamt: Double,
                     impagency: String,
                     lendinginstr: String,
                     lendinginstrtype: String,
                     lendprojectcost: Double,
                     majorsector_percent: List[Majorsector_percent],
                     mjsector_namecode: List[Mjsector_namecode],
                     mjtheme: List[String],
                     mjtheme_namecode: List[Mjsector_namecode],
                     mjthemecode: String,
                     prodline: String,
                     prodlinetext: String,
                     productlinetype: String,
                     project_abstract: Project_abstract,
                     project_name: String,
                     projectdocs: List[Projectdocs],
                     projectfinancialtype: String,
                     projectstatusdisplay: String,
                     regionname: String,
                     sector: List[Sector],
                     sector1: Majorsector_percent,
                     sector2: Majorsector_percent,
                     sector3: Majorsector_percent,
                     sector4: Majorsector_percent,
                     sector_namecode: List[Mjsector_namecode],
                     sectorcode: String,
                     source: String,
                     status: String,
                     supplementprojectflg: String,
                     theme1: Majorsector_percent,
                     theme_namecode: List[Mjsector_namecode],
                     themecode: String,
                     totalamt: Double,
                     totalcommamt: Double,
                     url: String
                   )

}

//val name = "venu"
