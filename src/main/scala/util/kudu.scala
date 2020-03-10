package main.scala.util

import java.util.Calendar

import org.apache.spark.sql.DataFrame

object kudu extends App {

  def insert(tableName:String,df:DataFrame): Unit ={

    kuduUtils.insert(df, tableName)
  }

  def insert13(tableName:String,df:DataFrame,date_s:String){

    val sdfMonth = new java.text.SimpleDateFormat("yyyyMM")
    val date = sdfMonth.parse(date_s)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MONTH,1)
    val date_e = sdfMonth.format(cal.getTime())
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq( "ORG_NO","ORG_TYPE","STAT_TIME"),List("ORG_NO","ORG_TYPE","STAT_TIME"), 16, List("STAT_TIME"))
    }
    if(!kuduUtils.partitionExists(tableName,date_s,date_e)){
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }

    kuduUtils.alterTableDropRangePartition(tableName,  "STAT_TIME", date_s, date_e)
    kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
    kuduUtils.upsert(df.repartition(16), tableName)
  }
  def insert12(tableName:String,df:DataFrame,date_s:String){

    val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")

    val date = sdfDay.parse(date_s)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_MONTH,1)
    val date_e = sdfDay.format(cal.getTime())
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq( "ORG_NO","CONS_NO","STAT_TIME"),List("ORG_NO","CONS_NO","STAT_TIME"), 16, List("STAT_TIME"))
    }
    if(!kuduUtils.partitionExists(tableName,date_s,date_e)){
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }

    kuduUtils.alterTableDropRangePartition(tableName,  "STAT_TIME", date_s, date_e)
    kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
    kuduUtils.upsert(df.repartition(16), tableName)
  }
  def insert(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")

    val date = sdfDay.parse(date_s)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_MONTH,1)
    val date_e = sdfDay.format(cal.getTime())
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq( "ORG_NO","ORG_TYPE","STAT_TIME"),List("ORG_NO","ORG_TYPE","STAT_TIME"), 16, List("STAT_TIME"))
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }

  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_TIME", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
 def insert1(tableName:String,df:DataFrame,date_s:String){
  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
        

    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("CONS_NO", "TG_ID", "ORG_NO", "STAT_DATE","METER_ID"),List("CONS_NO", "TG_ID", "ORG_NO","METER_ID"), 16, List("STAT_DATE"))
      kuduUtils.insert(df, "TEST")
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
    }
  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
 }
  def insert2(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
   
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("TG_ID", "STAT_DATE"),List("TG_ID"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName, "STAT_DATE", date_s, date_e)
      kuduUtils.upsert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName, "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName, "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
  def insert3(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
     
kuduUtils.dropTable(tableName)
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("ORG_NO","TG_ID","TG_NO" ,"STAT_DATE"),List("ORG_NO","TG_ID","TG_NO"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName, "STAT_DATE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName, "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName, "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
 def insert4(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
     

    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("DATA_TYPE","EXEC_CYCLE","METER_ID"),List("DATA_TYPE","METER_ID"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName, "EXEC_CYCLE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName, "EXEC_CYCLE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName, "EXEC_CYCLE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
 def insert5(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString


    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("ORG_NO","RESP_EMP_NO", "STAT_DATE"),List("ORG_NO","RESP_EMP_NO"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,"STAT_DATE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
     def insert6(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
     

    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("ORG_NO","ORG_TYPE", "STAT_DATE"),List("ORG_NO","ORG_TYPE"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
    
 def insert7(tableName:String,df:DataFrame,date_s:String){

   val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString


    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("TG_ID","CHANGE_TYPE","STAT_DATE"),List("TG_ID","CHANGE_TYPE"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName, "STAT_DATE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName, "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName, "STAT_DATE", date_s,date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }     
def insert8(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
        

    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("CONS_NO", "TG_NO", "ORG_NO", "STAT_DATE"),List("CONS_NO", "TG_NO", "ORG_NO"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }
def insert9(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString
        

    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("CONS_NO", "TG_NO", "EQUIP_ID","ORG_NO", "STAT_DATE"),List("CONS_NO","TG_NO","EQUIP_ID" ,"ORG_NO"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }

def insert10(tableName:String,df:DataFrame,date_s:String){

  val sdfDay = new java.text.SimpleDateFormat("yyyyMMdd")
     val date_e = (date_s.toLong +1).toString

kuduUtils.dropTable(tableName)
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq("ELEC_ADDR", "CONS_NO","MET_ASSET_NO","ATTR_CODE","OCCUR_TIME","STAT_DATE"),List( "ELEC_ADDR","ATTR_CODE","CONS_NO","OCCUR_TIME","MET_ASSET_NO"), 16, List("STAT_DATE"))
     
    }
      if(!kuduUtils.partitionExists(tableName,date_s,date_e)){ 
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
      kuduUtils.upsert(df, tableName)
    }
  kuduUtils.alterTableDropRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.alterTableAddRangePartition(tableName,  "STAT_DATE", date_s, date_e)
  kuduUtils.upsert(df.repartition(16), tableName) 
  }

  def insert11(tableName:String,df:DataFrame,date_s:String) {

    val sdfMonth = new java.text.SimpleDateFormat("yyyyMM")
    val date = sdfMonth.parse(date_s)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MONTH,1)
    val date_e = sdfMonth.format(cal.getTime())
    if(!kuduUtils.tableExists(tableName))
    {
      kuduUtils.createHashPartitionTableWithRange(tableName, df, Seq( "STAT_TIME","ORG_NO","CONS_NO"),List("STAT_TIME","ORG_NO","CONS_NO"), 16, List("STAT_TIME"))
    }
    if(!kuduUtils.partitionExists(tableName,date_s,date_e)){
      kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
      kuduUtils.insert(df, tableName)
    }

    kuduUtils.alterTableDropRangePartition(tableName,  "STAT_TIME", date_s, date_e)
    kuduUtils.alterTableAddRangePartition(tableName,  "STAT_TIME", date_s, date_e)
    kuduUtils.upsert(df.repartition(16), tableName)
  }

}