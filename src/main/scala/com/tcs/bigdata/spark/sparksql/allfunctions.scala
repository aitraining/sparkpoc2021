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

  val ourl ="jdbc:oracle:thin:@//myoracledb.conadtfguis7.ap-south-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new java.util.Properties()
  oprop.setProperty("user","ousername")
  oprop.setProperty("password","opassword")
 // oprop.setProperty("fetchsize","1000") // its use huge amount of resources
 // oprop.setProperty("batchsize","10000")// same like fetchsize, applicable write
 // oprop.setProperty("sessionInitStatement","delete from asl where pk=null")
 // oprop.setProperty("truncate","true") // instread of drop table, truncate data (structure still existing)
  //result.write.mode(SaveMode.Overwrite).jdbc(ourl,"result",oprop)
  //overwrite ...backend .. drop table, and recreate table ..
  //create tab test(name varchar(128), age long); before
  // drop test create again ... create table test(name varchar(32), age int)
  oprop.setProperty("driver","oracle.jdbc.OracleDriver")
 // oprop.setProperty("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024), age long") // oracle must support datatype
 // oprop.setProperty("customSchema","name String, comments String, age Byte") //spark point of view
 // oprop.setProperty("createTableOptions","")

}