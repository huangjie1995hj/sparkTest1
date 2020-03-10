package main.scala.util

import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

object kuduUtils {
  private val kconfig = "hadoop122.nari.com:7051,hadoop123.nari.com:7051,hadoop44.nari.com:7051"
  private val kc = new KuduContext(kconfig,
    SparkContext.getOrCreate())

  private lazy val kClient = kc.syncClient

  private def setNullableSchema(df: DataFrame, cols: Seq[String], nullable: Boolean) = {
    StructType(df.schema.map {
      case StructField(c, t, _, m) if cols.contains(c) => StructField(c, t, nullable, m)
      case y: StructField                              => y
    })
  }

  def tableExists(tableName: String) = kc.tableExists(tableName)

  def read(hc: HiveContext, table: String) = {
    hc.read.options(
      Map("kudu.master" -> kconfig,
        "kudu.table" -> table)).kudu
  }

  def createTable(tableName: String, df: DataFrame, keys: Seq[String], ct: CreateTableOptions) {
    kc.createTable(tableName, setNullableSchema(df, keys, false), keys, ct)
  }

  def createRangePartitionTable(tableName: String, df: DataFrame, keys: Seq[String], rangeKey: List[String]) {
    kc.createTable(tableName, setNullableSchema(df, keys, false), keys,
      new CreateTableOptions().setRangePartitionColumns(rangeKey))
    alterTableDropAllRangePartition(tableName)
  }

  def createHashPartitionTable(tableName: String, df: DataFrame, keys: Seq[String], hashKeys: List[String], hashBuckets: Int) {
    kc.createTable(tableName, setNullableSchema(df, keys, false), keys,
      new CreateTableOptions().addHashPartitions(hashKeys, hashBuckets))
  }

  def createHashPartitionTableWithRange(tableName: String, df: DataFrame, keys: Seq[String], hashKeys: List[String],
                                        hashBuckets: Int, rangeKey: List[String]) {
    kc.createTable(tableName, setNullableSchema(df, keys, false), keys,
      new CreateTableOptions().addHashPartitions(hashKeys, hashBuckets).setRangePartitionColumns(rangeKey))
    alterTableDropAllRangePartition(tableName)
  }

  def alterTable(tableName: String, at: AlterTableOptions) {
    kClient.alterTable(tableName, at)
  }

  def getDateStrUpperBound(date: String) = (date.toLong + 1).toString

  def getFormattedRangePartitions(tableName: String) = {
    kClient.openTable(tableName).getFormattedRangePartitions(600000)
  }

  def partitionExists(tableName: String, lower: String, upper: String) = {
    val partitions = getFormattedRangePartitions(tableName).toSeq.map(_.replaceAll("\"", "")
      .replaceAll(" <= VALUES < ", ","))
    partitions.contains(s"$lower,$upper")
  }

  def alterTableAddRangePartition(tableName: String, partitionCol: String, lower: String, upper: String) {
    val schema = kClient.openTable(tableName).getSchema
    val lowerBound = schema.newPartialRow()
    val upperBound = schema.newPartialRow()
    lowerBound.addString(partitionCol, lower)
    upperBound.addString(partitionCol, upper)
    kClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lowerBound, upperBound))
  }

  def alterTableDropRangePartition(tableName: String, partitionCol: String, lower: String, upper: String) {
    val schema = kClient.openTable(tableName).getSchema
    val lowerBound = schema.newPartialRow()
    val upperBound = schema.newPartialRow()
    lowerBound.addString(partitionCol, lower)
    upperBound.addString(partitionCol, upper)
    kClient.alterTable(tableName, new AlterTableOptions().dropRangePartition(lowerBound, upperBound))
  }

  def alterTableAddRangePartition(tableName: String, partitionCol: String, lower: Long, upper: Long) {
    val schema = kClient.openTable(tableName).getSchema
    val lowerBound = schema.newPartialRow()
    val upperBound = schema.newPartialRow()
    lowerBound.addLong(partitionCol, lower * 1000)
    upperBound.addLong(partitionCol, upper * 1000)
    kClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lowerBound, upperBound))
  }

  def alterTableDropRangePartition(tableName: String, partitionCol: String, lower: Long, upper: Long) {
    val schema = kClient.openTable(tableName).getSchema
    val lowerBound = schema.newPartialRow()
    val upperBound = schema.newPartialRow()
    lowerBound.addLong(partitionCol, lower * 1000)
    upperBound.addLong(partitionCol, upper * 1000)
    kClient.alterTable(tableName, new AlterTableOptions().dropRangePartition(lowerBound, upperBound))
  }

  def alterTableDropAllRangePartition(tableName: String) {
    val schema = kClient.openTable(tableName).getSchema
    val lowerBound = schema.newPartialRow()
    val upperBound = schema.newPartialRow()
    kClient.alterTable(tableName, new AlterTableOptions().dropRangePartition(lowerBound, upperBound))
  }

  def dropTable(tableName: String) {
    kc.deleteTable(tableName)
  }

  def insert(df: DataFrame, tableName: String) {
    kc.insertRows(df, tableName)
  }

  def delete(df: DataFrame, tableName: String) {
    kc.deleteRows(df, tableName)
  }

  def upsert(df: DataFrame, tableName: String) {
    kc.upsertRows(df, tableName)
  }

  def update(df: DataFrame, tableName: String) {
    kc.updateRows(df, tableName)
  }
  
  
  
}