/*
package main.scala.sparkTask1
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import org.apache.kudu.client._

import collection.JavaConverters._


object SparkTest {
  //kuduMasters and tableName
  val kuduMasters = "192.168.13.130:7051"
  val tableName = "kudu_spark_table"

  //table column
  val idCol = "id"
  val ageCol = "age"
  val nameCol = "name"

  //replication
  val tableNumReplicas = Integer.getInteger("tableNumReplicas", 1)

  val logger = LoggerFactory.getLogger(SparkTest.getClass)

  def main(args: Array[String]): Unit = {
    //create SparkSession
    val spark = SparkSession.builder().appName("KuduApp").master("local[2]").getOrCreate()

    //create kuduContext
    val kuduContext = new KuduContext(kuduMasters,spark.sparkContext)

    //schema
    val schema = StructType(
      List(
        StructField(idCol, IntegerType, false),
        StructField(nameCol, StringType, false),
        StructField(ageCol,StringType,false)
      )
    )

    var tableIsCreated = false
    try{
      // Make sure the table does not exist
      if (kuduContext.tableExists(tableName)) {
        throw new RuntimeException(tableName + ": table already exists")
      }

      //create

      kuduContext.createTable(tableName, schema, Seq(idCol),
        new CreateTableOptions()
          .addHashPartitions(List(idCol).asJava, 3)
          .setNumReplicas(tableNumReplicas))

      tableIsCreated = true
      import spark.implicits._

      //write
      logger.info(s"writing to table '$tableName'")

      val data = Array(Person(1,"12","zhangsan"),Person(2,"20","lisi"),Person(3,"30","wangwu"))

      val personRDD = spark.sparkContext.parallelize(data)


      val personDF = personRDD.toDF()

      kuduContext.insertRows(personDF,tableName)

      //useing SparkSQL read table
      val sqlDF = spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMasters, "kudu.table" -> tableName))
        .format("kudu").kudu
      sqlDF.createOrReplaceTempView(tableName)

      spark.sqlContext.sql(s"SELECT * FROM $tableName ").show


      //upsert some rows
      val upsertPerson = Array(Person(1,"10","jack"))
      val upsertPersonRDD = spark.sparkContext.parallelize(upsertPerson)
      val upsertPersonDF = upsertPersonRDD.toDF()
      kuduContext.updateRows(upsertPersonDF,tableName)


      //useing RDD read table
      val readCols = Seq(idCol,ageCol,nameCol)
      val readRDD = kuduContext.kuduRDD(spark.sparkContext, tableName, readCols)
      val userTuple = readRDD.map { case Row( id: Int,age: String,name: String) => (id,age,name) }
      println("count:"+userTuple.count())
      userTuple.collect().foreach(println(_))

      //delete table
      kuduContext.deleteTable(tableName)
    }catch {
      // Catch, log and re-throw. Not the best practice, but this is a very
      // simplistic example.
      case unknown : Throwable => logger.error(s"got an exception: " + unknown)
        throw unknown
    } finally {
      // Clean up.
      if (tableIsCreated) {
        logger.info(s"deleting table '$tableName'")
        kuduContext.deleteTable(tableName)
      }
      logger.info(s"closing down the session")
      spark.close()
    }
  }

}

case class Person(id: Int,age: String,name: String)

*/
