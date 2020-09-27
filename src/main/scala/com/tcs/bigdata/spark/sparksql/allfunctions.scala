package com.tcs.bigdata.spark.sparksql

object allfunctions {
  def conc(fname:String, lname:String) = {fname.toUpperCase() + " " + lname}
  def offers(age:Int) = age match {
    case x if(x>0 && x<10) => "10% off"
    case x if(x>=10 && x<=18) => "15% off"
    case x if(x>18 && x<50) => "20% off"
    case x if(x>=50 && x<60) => "70% off"
    case _ => "no offer"
  }
  def stateoffers(st:String) = st match {
    case "OH" => "10% off"
    case "NJ" => "11% off"
    case "LA" => "20% off"
    case _ => "no offer"
  }
  val msurl ="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
  val msprop = new java.util.Properties()
  msprop.setProperty("user","msuername")
  msprop.setProperty("password","mspassword")
  msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

  val ourl ="jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new java.util.Properties()
  oprop.setProperty("user","ousername")
  oprop.setProperty("password","opassword")
  oprop.setProperty("driver","oracle.jdbc.OracleDriver")

}