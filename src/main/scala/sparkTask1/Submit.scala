package main.scala.sparkTask1

import org.apache.kudu.client.{AlterTableOptions, KuduClient}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.util._

object Submit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("conf")
    val context = new SparkContext(conf)
    val context1 = new HiveContext(context)
    import context1.implicits._
    val dataFrame = context1.read
      .format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", true.toString)
      .load(s"""/temp/a_cons_violate_d1227.csv""")
    print(dataFrame.show(2))
    dataFrame.printSchema()
    val df = dataFrame.select(
      $"ORG_NO".cast("String"),
      $"ORG_TYPE".cast("String"),
      $"ORG_NAME".cast("String"),
      $"ORG_CITY_NO".cast("String"),
      $"ORG_CITY_NAME".cast("String"),
      $"ORG_COUNTY_NO".cast("String"),
      $"ORG_COUNTY_NAME".cast("String"),
      $"CONS_NO".cast("String"),
      $"CONS_NAME".cast("String"),
      $"CONS_SORT".cast("String"),
      $"TG_NO".cast("String"),
      $"TG_NAME".cast("String"),
      $"VOLT_CODE".cast("String"),
      $"RUN_CAP".cast("decimal(16, 6)"),
      $"ELEC_TYPE_CODE".cast("String"),
      $"TRADE_CODE".cast("String"),
      $"TRADE_NAME".cast("String"),
      $"A00112".cast("String"),
      $"A00112_DURING".cast("decimal(16, 2)"),
      $"A00112_CNT".cast("decimal(26, 0)"),
      $"A00100".cast("String"),
      $"A00100_DURING".cast("decimal(16, 2)"),
      $"A00100_CNT".cast("decimal(26, 0)"),
      $"A0010J".cast("String"),
      $"A0010J_DURING".cast("decimal(16, 2)"),
      $"A0010J_CNT".cast("decimal(26, 0)"),
      $"A00102".cast("String"),
      $"A00102_DURING".cast("decimal(16, 2)"),
      $"A00102_CNT".cast("decimal(26, 0)"),
      $"A00113".cast("String"),
      $"A00113_DURING".cast("decimal(16, 2)"),
      $"A00113_CNT".cast("decimal(26, 0)"),
      $"A00114".cast("String"),
      $"A00114_DURING".cast("decimal(16, 2)"),
      $"A00114_CNT".cast("decimal(26, 0)"),
      $"A00115".cast("String"),
      $"A00115_DURING".cast("decimal(16, 2)"),
      $"A00115_CNT".cast("decimal(26, 0)"),
      $"A00116".cast("String"),
      $"A00116_DURING".cast("decimal(16, 2)"),
      $"A00116_CNT".cast("decimal(26, 0)"),
      $"A00118".cast("String"),
      $"A00118_DURING".cast("decimal(16, 2)"),
      $"A00118_CNT".cast("decimal(26, 0)"),
      $"A00119".cast("String"),
      $"A00119_DURING".cast("decimal(16, 2)"),
      $"A00119_CNT".cast("decimal(26, 0)"),
      $"A0011B".cast("String"),
      $"A0011B_DURING".cast("decimal(16, 2)"),
      $"A0011B_CNT".cast("decimal(26, 0)"),
      $"A0011E".cast("String"),
      $"A0011E_DURING".cast("decimal(16, 2)"),
      $"A0011E_CNT".cast("decimal(26, 0)"),
      $"A0011J".cast("String"),
      $"A0011J_DURING".cast("decimal(16, 2)"),
      $"A0011J_CNT".cast("decimal(26, 0)"),
      $"OPEN_COVER_CNT".cast("String"),
      $"START_BUTTON_COVER_CNT".cast("String"),
      $"VIOLATE_FLAG".cast("String"),
      $"EQUIP_DEFECT_FLAG".cast("String"),
      $"MR_EXCP_FLAG".cast("String"),
      $"STAT_TIME".cast("String")
    )
    df.printSchema()
    val ctxt = new KuduContext("192.168.176.15:7051", context)
    lazy val kClient = ctxt.syncClient
    //alterTableAddRangePartition(kClient, "A_CONS_VIOLATE_D", "STAT_TIME", "20191227", "20191228")
    //kuduContext.upsertRows(dataFrame, "a_cons_violate_d")
    kudu.insert12( "A_CONS_VIOLATE_D",df,"20191227")
    true
    print(">>>>>>>>>>>>>>>>>>末尾")
  }

  def alterTableAddRangePartition(kClient: KuduClient, tableName: String, partitionCol: String, lower: String, upper: String) {
    val schema = kClient.openTable(tableName).getSchema
    val lowerBound = schema.newPartialRow()
    val upperBound = schema.newPartialRow()
    lowerBound.addString(partitionCol, lower)
    upperBound.addString(partitionCol, upper)
    kClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lowerBound, upperBound))
  }

}
