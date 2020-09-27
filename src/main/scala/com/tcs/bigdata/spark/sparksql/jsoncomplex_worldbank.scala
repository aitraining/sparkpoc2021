package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.tcs.bigdata.spark.sparksql.allfunctions._
object jsoncomplex_worldbank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsoncomplex").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\jsondata\\world_bank\\world_bank.json"
    val df = spark.read.format("json").load(data)
    df.createOrReplaceTempView("tab")
    val query =("select  _id.`$oid` OID, approvalfy,board_approval_month,boardapprovaldate, " +
      "borrower,closingdate,country_namecode,countrycode," +
      "supplementprojectflg,countryname,countryshortname," +
      " envassesmentcategorycode,grantamt,ibrdcommamt,id,idacommamt,impagency,lendinginstr, lendinginstrtype,lendprojectcost, mjthemecode,sector.name[0] name1,sector.name[1] name2, sector.name[2] name3," +
      " sector1.name S1Name, sector1.percent S1Percent," +
      "sector2.name S2Name, sector2.percent S2Percent," +
      "sector3.name S3Name, sector3.percent S3Percent," +
      "sector4.name S4Name, sector4.percent S4Percent, prodline, prodlinetext, productlinetype,"+
      " mjtheme[0] mjtheme1,mjtheme[1] mjtheme2,mjtheme[2] mjtheme3  , project_name ,projectfinancialtype, projectstatusdisplay,regionname,sectorcode, source,status,themecode,totalamt,totalcommamt,url," +
      "mp.Name mpname, mp.Percent mppercent, tn.code tncode, tn.name tnname,mn.code mncode, mn.name mnname, tc.code tccode, tc.name tcname, pd.DocDate pddocdate ,pd.DocType pddoctype, pd.DocTypeDesc pddoctypedesc, pd.DocURL  pddocurl, pd.EntityID pdid from tab lateral view explode(majorsector_percent) tmp as mp lateral view explode(theme_namecode) tmp as tn lateral view explode(mjsector_namecode) tmp as mn lateral view explode(mjtheme_namecode) tmp as tc lateral view explode(projectdocs) tmp as pd")

   // val res = spark.sql(query)
   // res.printSchema()
    //df.createOrReplaceTempView("tab")
    // if u have struct ... use parent_col.child_col let eg: theme1 its struct ..... theme1.Name, theme1.Percent
    // if you have struct within array .. use explode ... let eg: sector_namecode: array (nullable = true)
    // |    |-- element: struct (containsNull = true)
    // |    |    |-- code: string (nullable = true)
    // |    |    |-- name: string (nullable = true)
    // use explode(sector_namecode) ... now explode remove array ... sector_namecode.code, sector_namecode.nametheme_namecode
    val res = df.withColumn("sn", explode($"sector_namecode")).withColumn("sec", explode($"sector")).withColumn("pd", explode($"projectdocs")).withColumn("mnc",explode($"mjtheme_namecode")).withColumn("mjt", explode($"mjtheme")).withColumn("msnc", explode($"mjsector_namecode")).withColumn("mjp",explode($"majorsector_percent")).withColumn("mjtheme",explode($"mjtheme"))
      //.select( $"sec.Name".as("secname"), $"regionname", $"projectstatusdisplay", $"projectfinancialtype", $"sector1.Name".as("sector1name"), $"sector1.Percent".as("sector1percent"), $"sector2.Name".as("sector2name"), $"sector2.Percent".as("sector2percent"), $"sector3.Name".as("sector3name"), $"sector3.Percent".as("sector3percent"), $"sector4.Name".as("sector4name"), $"sector3.Percent".as("sector4percent"), $"sn.name".as("sector_namecodename"), $"sn.code".as("sector_namecodecode"),$"url", $"totalcommamt",$"totalamt",$"themecode", col("theme1.Name").as("themename"),col("theme1.Percent").as("themepercent"), $"supplementprojectflg",$"status",$"source",$"sectorcode")
      .select(  $"mjtheme".as("mjthemevalues"),$"_id".getItem("$oid").as("idoid"), $"approvalfy",$"board_approval_month",$"boardapprovaldate",$"borrower",$"closingdate",$"country_namecode",$"countrycode",$"countryname",$"countryshortname",$"docty",$"envassesmentcategorycode",$"grantamt",$"ibrdcommamt",$"id",$"idacommamt",$"impagency",$"lendinginstr",$"lendinginstrtype",$"mjt", $"msnc.name".as("msncname"), $"msnc.code".as("msnccode"), $"mjp.Name".as("mjpname"), $"mjp.Percent".as("mjppercent"), $"mnc.code".as("mnccode"),$"mnc.name".as("mncname"),$"project_abstract.cdata",$"productlinetype",$"prodlinetext",$"prodline",$"mjthemecode", $"pd.DocDate",$"pd.DocType", $"pd.DocTypeDesc",$"pd.DocURL",$"pd.EntityID", $"project_name", $"sec.Name".as("secname"), $"regionname", $"projectstatusdisplay", $"projectfinancialtype", $"sector1.Name".as("sector1name"), $"sector1.Percent".as("sector1percent"), $"sector2.Name".as("sector2name"), $"sector2.Percent".as("sector2percent"), $"sector3.Name".as("sector3name"), $"sector3.Percent".as("sector3percent"), $"sector4.Name".as("sector4name"), $"sector3.Percent".as("sector4percent"), $"sn.name".as("sector_namecodename"), $"sn.code".as("sector_namecodecode"),$"url", $"totalcommamt",$"totalamt",$"themecode", col("theme1.Name").as("themename"),col("theme1.Percent").as("themepercent"), $"supplementprojectflg",$"status",$"source",$"sectorcode")

    // |-- mjtheme: array (nullable = true)
    // |    |-- element: string (containsNull = true)
    // |-- in MJtheme u have array if u have only array use flattern its good


 //   res.write.jdbc(ourl, "wordbankjsontab",oprop)

    res.show()
//df.printSchema()
    res.printSchema()
//df.printSchema()
    /*  val res = spark.sql("select _id id, city, loc[0] lang, loc[1] lati, pop, state  from tab")
  res.write.jdbc(ohost, "madhurazip",oprop) // if u get class not found error pls add ojdbc7.jar in intellij
  */
    /*  val res = df.withColumn("loc",explode($"loc")).withColumnRenamed("_id","id")
        .select("id","city", "pop", "state", "loc")*/
    // res.show()
    //    df.show()

    spark.stop()
  }
}