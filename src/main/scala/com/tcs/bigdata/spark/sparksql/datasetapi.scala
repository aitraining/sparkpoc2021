package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case class _id(
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
case class wbcc(
                 _id: _id,
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

object datasetapi {
  case class uscc(first_name: String, last_name: String, company_name:String, address:String, city:String, county:String, state: String, zip: Int, phone1:String, phone2:String, email:String, web: String)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datasetapi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.show()

/*
    val qry =
      """CREATE TABLE my_table(a string, b string, ...)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |WITH SERDEPROPERTIES (
        |   "separatorChar" = ",",
        |   "quoteChar"     = "'",
        |   "escapeChar"    = "\\"
        |)
        |STORED AS TEXTFILE;
        |""".stripMargin

spark.sql(qry)
*/

    //df.write.insertInto("my_table")
    val op = "file:///C:\\work\\datasets\\output\\us500telda"
    df.write.format("csv").option("delimiter","~").save(op)

    //val ds = df.as[uscc]

   // val res = spark.sql("select *, cast(regex_replace('phone1','-','') as int) phone1, from tab where email like '%gmail.com%'")
    spark.stop()
  }
}