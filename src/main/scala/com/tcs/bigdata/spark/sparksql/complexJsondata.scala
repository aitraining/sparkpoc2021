package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexJsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexJsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    // mobiles, social media, cloud most of servers generate json data.
    //light weight format ..
val data = "C:\\work\\datasets\\jsondata\\world_bank\\world_bank.json"
    val df = spark.read.format("json").load(data)

    df.createOrReplaceTempView("tab")
    //data cleaning processing
    val query = """select `_id`.`$oid` idoid, approvalfy,board_approval_month,boardapprovaldate,borrower,closingdate,country_namecode,countrycode,countryname,countryshortname,docty,envassesmentcategorycode,grantamt, ibrdcommamt, id,idacommamt, impagency,lendinginstr,lendinginstrtype,lendprojectcost, sector1.Name sector1name, sector1.Percent sector1percent,sector2.Name sector2name, sector2.Percent sector2percent, sector3.Name sector3name, sector3.Percent sector3percent,sector4.Name sector4name, sector4.Percent sector4percent, mp.Name mpname, mp.Percent mppercent, mn.name mnname, mn.code mncode, mtn.name mtnname, mtn.code mtncode,mjthemecode, prodline, prodlinetext, productlinetype, projectfinancialtype, projectstatusdisplay, regionname, sectorcode, source, status, supplementprojectflg, themecode, totalamt, totalcommamt, url, thnc.code thnccode, thnc.name thncname, theme1.Name theme1name, theme1.Percent theme1percent, snc.name sncname, snc.code snccode, sec.Name secname, pd.DocDate, pd.DocType, pd.DocTypeDesc, pd.DocURL, pd.EntityID, project_name, project_abstract.cdata projectcdata , mjt from tab lateral view explode(majorsector_percent) t as mp lateral view explode(mjsector_namecode) t as mn lateral view explode(mjtheme_namecode) t as mtn  lateral view explode(theme_namecode) t as thnc lateral view explode(sector_namecode) t as snc  lateral view explode(sector) t as sec lateral view explode(projectdocs) t as pd lateral view explode(mjtheme) t as mjt"""

    val res = spark.sql(query)
    // data processing
    res.createOrReplaceTempView("table")
    val result = spark.sql("select borrower, count(*) cnt from table group by borrower order by cnt desc")
    result.show()
    val op = ""
    result.write.format("csv").option("header","true").save(op)
//    res.printSchema()
    spark.stop()
  }
}