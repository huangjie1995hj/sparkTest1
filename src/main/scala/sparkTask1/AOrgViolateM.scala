package main.scala.sparkTask1

import java.util.{Calendar, Date, Properties}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.kudu.spark.kudu._
import main.scala.util._
import org.apache.spark.sql.Dataset

object AOrgViolateM {

  private val kconfig = "192.168.176.15:7051"
  private val pro = new Properties()

  def AOrgViolateM(hc:HiveContext,dataDate:String): Boolean ={
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>dataDate="+dataDate)

    import  hc.implicits._
    // val context = new KuduContext("192.168.176.15:7051",spark.sparkContext)
    //val dataset = hc.sql("select * from kd.A_CONS_VIOLATE_D where STAT_TIME = 20191227")
    val dataset =  kuduUtils.read(hc,
        "A_CONS_VIOLATE_M"
    ).where($"STAT_TIME" === dataDate)
    println(">>>>>>>>>>>>>>>>>>>>>>>违规用电窃电日统计表数据=")
    dataset.show(2)
   /* def getColumn(column:String): String ={
      if("day".equals(dateType)){
        column
      }else{
        column.replace("CNT","DAY_CNT")
      }
    }*/

    val data1 = dataset.toDF().withColumn("FIRSTLEVELCAP",when($"RUN_CAP" <500,1))
      .withColumn("SECONDLEVELCAP",when($"RUN_CAP">=500 && $"RUN_CAP" < 1000,1))
      .withColumn("THIRDLEVELCAP",when($"RUN_CAP" >= 1000 && $"RUN_CAP" <2000,1))
      .withColumn("FOURTHLEVELCAP",when($"RUN_CAP" >= 2000 && $"RUN_CAP" < 3000,1))
      .withColumn("FIFTHLEVELCAP",when($"RUN_CAP" > 3000,1))
      .withColumn("FIRSTCONSSORT",when($"CONS_SORT" === "1",1))
      .withColumn("SECONDCONSSORT",when($"CONS_SORT" === "2",1))
      .withColumn("THIRDCONSSORT",when($"CONS_SORT"==="3",1))
      .withColumn("FOURTHCONSSORT",when($"CONS_SORT"==="4",1))
      .withColumn("FIFTHCONSSORT",when($"CONS_SORT"==="5",1))
      .withColumn("SIXTHCONSSORT",when($"CONS_SORT"==="6",1))
      .withColumn("SEVENTHCONSSORT",when($"CONS_SORT"==="7",1))
      .withColumn("EIGHTHCONSSORT",when($"CONS_SORT"==="8",1))
      .withColumn("NINTHCONSSORT",when($"CONS_SORT"==="9",1))
      .withColumn("TENTHCONSSORT",when($"CONS_SORT"==="10",1))
      .withColumn("ANTISTEALINGPROFIRST",when($"ANTISTEALINGPRO" >= 0 && $"ANTISTEALINGPRO" <0.2,value = 1))
      .withColumn("ANTISTEALINGPROSECOND",when($"ANTISTEALINGPRO" >= 0.2 && $"ANTISTEALINGPRO" <0.4,value = 1))
      .withColumn("ANTISTEALINGPROTHIRD",when($"ANTISTEALINGPRO" >= 0.4 && $"ANTISTEALINGPRO" <0.6,value = 1))
      .withColumn("ANTISTEALINGPROFOURTH",when($"ANTISTEALINGPRO" >= 0.6,value = 1))
    //.groupBy("ORG_NO", "CONS_NO")
    //1.将A_CONS_VIOLATE_D 按供电单位合计
    val frame = data1.groupBy($"ORG_NO",$"ORG_TYPE",$"ORG_NAME",$"STAT_TIME").agg(
      sum($"RUN_CAP") as "RUN_CAP",
      avg($"A00112_DURING") as "A00112_DURING",
      sum("A00112_DAY_CNT") as "A00112_DAY_CNT" ,
      avg($"A00100_DURING") as "A00100_DURING",
      sum("A00100_DAY_CNT") as "A00100_DAY_CNT" ,
      avg($"A0010J_DURING") as "A0010J_DURING",
      sum("A0010J_DAY_CNT") as "A0010J_DAY_CNT" ,
      avg($"A00102_DURING") as "A00102_DURING",
      sum("A00102_DAY_CNT") as "A00102_DAY_CNT" ,
      avg($"A00113_DURING") as "A00113_DURING",
      sum("A00113_DAY_CNT") as "A00113_DAY_CNT" ,
      avg($"A00114_DURING") as "A00114_DURING",
      sum("A00114_DAY_CNT") as "A00114_DAY_CNT" ,
      avg($"A00115_DURING") as "A00115_DURING",
      sum("A00115_DAY_CNT") as "A00115_DAY_CNT" ,
      avg($"A00116_DURING") as "A00116_DURING",
      sum("A00116_DAY_CNT") as "A00116_DAY_CNT" ,
      avg($"A00118_DURING") as "A00118_DURING",
      sum("A00118_DAY_CNT") as "A00118_DAY_CNT" ,
      avg($"A00119_DURING") as "A00119_DURING",
      sum("A00119_DAY_CNT") as "A00119_DAY_CNT" ,
      avg($"A0011B_DURING") as "A0011B_DURING",
      sum("A0011B_DAY_CNT") as "A0011B_DAY_CNT" ,
      avg($"A0011E_DURING") as "A0011E_DURING",
      sum("A0011E_DAY_CNT") as "A0011E_DAY_CNT" ,
      avg($"A0011J_DURING") as "A0011J_DURING",
      sum("A0011J_DAY_CNT") as "A0011J_DAY_CNT",
      sum("FIRSTLEVELCAP") as "FIRST_LEVEL_CAP_CNT",
      sum("SECONDLEVELCAP") as "SECOND_LEVEL_CAP_CNT",
      sum("THIRDLEVELCAP") as "THIRD_LEVEL_CAP_CNT",
      sum("FOURTHLEVELCAP") as "FOURTH_LEVEL_CAP_CNT",
      sum("FIFTHLEVELCAP") as "FIFTH_LEVEL_CAP_CNT",
      sum("FIRSTCONSSORT") as "FIRST_CONS_SORT_CNT",
      sum("SECONDCONSSORT") as "SECOND_CONS_SORT_CNT",
      sum("THIRDCONSSORT") as "THIRD_CONS_SORT_CNT",
      sum("FOURTHCONSSORT") as "FOURTH_CONS_SORT_CNT",
      sum("FIFTHCONSSORT") as "FIFTH_CONS_SORT_CNT",
      sum("SIXTHCONSSORT") as "SIXTH_CONS_SORT_CNT",
      sum("SEVENTHCONSSORT") as "SEVENTH_CONS_SORT_CNT",
      sum("EIGHTHCONSSORT") as "EIGHTH_CONS_SORT_CNT",
      sum("NINTHCONSSORT") as "NINTH_CONS_SORT_CNT",
      sum("TENTHCONSSORT") as "TENTH_CONS_SORT_CNT",
      sum("ANTISTEALINGPROFIRST") as "ANTISTEALINGPROFIRST_CNT",
      sum("ANTISTEALINGPROSECOND") as "ANTISTEALINGPROSECOND_CNT",
      sum("ANTISTEALINGPROTHIRD") as "ANTISTEALINGPROTHIRD_CNT",
      sum("ANTISTEALINGPROFOURTH") as "ANTISTEALINGPROFOURTH_CNT"
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    frame.show(2)
    // 获取a_stat_org
    pro.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    pro.setProperty("user", "ocean_arch")
    pro.setProperty("password", "sea3000")
    val O_ORG_CHILDS_NEW = hc.read.jdbc("jdbc:oracle:thin:@192.168.176.25:1521/sea", "O_ORG_CHILDS_NEW", pro).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(">>>>>>>>>>>>>>>>>>>>>>>供电单位下级关系表数据=")
    O_ORG_CHILDS_NEW.show(2)

    //1.获取所的数据
    val supplyData = frame.filter($"ORG_TYPE" === "5").drop("ORG_TYPE").withColumn("ORG_TYPE",lit("05"))
      .select(
        "ORG_NO",
        "ORG_TYPE",
        "STAT_TIME",
        "RUN_CAP",
        "A00112_DURING",
        "A00112_DAY_CNT",
        "A00100_DURING",
        "A00100_DAY_CNT",
        "A0010J_DURING",
        "A0010J_DAY_CNT",
        "A00102_DURING",
        "A00102_DAY_CNT",
        "A00113_DURING",
        "A00113_DAY_CNT",
        "A00114_DURING",
        "A00114_DAY_CNT",
        "A00115_DURING",
        "A00115_DAY_CNT",
        "A00116_DURING",
        "A00116_DAY_CNT",
        "A00118_DURING",
        "A00118_DAY_CNT",
        "A00119_DURING",
        "A00119_DAY_CNT",
        "A0011B_DURING",
        "A0011B_DAY_CNT",
        "A0011E_DURING",
        "A0011E_DAY_CNT",
        "A0011J_DURING",
        "A0011J_DAY_CNT",
        "FIRST_LEVEL_CAP_CNT",
        "SECOND_LEVEL_CAP_CNT",
        "THIRD_LEVEL_CAP_CNT",
        "FOURTH_LEVEL_CAP_CNT",
        "FIFTH_LEVEL_CAP_CNT",
        "FIRST_CONS_SORT_CNT",
        "SECOND_CONS_SORT_CNT",
        "THIRD_CONS_SORT_CNT",
        "FOURTH_CONS_SORT_CNT",
        "FIFTH_CONS_SORT_CNT",
        "SIXTH_CONS_SORT_CNT",
        "SEVENTH_CONS_SORT_CNT",
        "EIGHTH_CONS_SORT_CNT",
        "NINTH_CONS_SORT_CNT",
        "TENTH_CONS_SORT_CNT",
        "ANTISTEALINGPROFIRST_CNT",
        "ANTISTEALINGPROSECOND_CNT",
        "ANTISTEALINGPROTHIRD_CNT",
        "ANTISTEALINGPROFOURTH_CNT"
      )
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(">>>>>>>>>>>>>>>>>>>>>>>所统计数据量="+supplyData.count())
    //1.获取县数据
    val townData = frame.as("frame").join(O_ORG_CHILDS_NEW.filter($"ORG_TYPE" === "04").as("org"),
      $"frame.ORG_NO" === $"org.CHILD_ORG_NO").groupBy($"org.ORG_NO",$"org.ORG_TYPE",$"frame.STAT_TIME")
      .agg(
        sum($"RUN_CAP") as "RUN_CAP",
        avg($"A00112_DURING") as "A00112_DURING",
        sum("A00112_DAY_CNT") as "A00112_DAY_CNT" ,
        avg($"A00100_DURING") as "A00100_DURING",
        sum("A00100_DAY_CNT") as "A00100_DAY_CNT" ,
        avg($"A0010J_DURING") as "A0010J_DURING",
        sum("A0010J_DAY_CNT") as "A0010J_DAY_CNT" ,
        avg($"A00102_DURING") as "A00102_DURING",
        sum("A00102_DAY_CNT") as "A00102_DAY_CNT" ,
        avg($"A00113_DURING") as "A00113_DURING",
        sum("A00113_DAY_CNT") as "A00113_DAY_CNT" ,
        avg($"A00114_DURING") as "A00114_DURING",
        sum("A00114_DAY_CNT") as "A00114_DAY_CNT" ,
        avg($"A00115_DURING") as "A00115_DURING",
        sum("A00115_DAY_CNT") as "A00115_DAY_CNT" ,
        avg($"A00116_DURING") as "A00116_DURING",
        sum("A00116_DAY_CNT") as "A00116_DAY_CNT" ,
        avg($"A00118_DURING") as "A00118_DURING",
        sum("A00118_DAY_CNT") as "A00118_DAY_CNT" ,
        avg($"A00119_DURING") as "A00119_DURING",
        sum("A00119_DAY_CNT") as "A00119_DAY_CNT" ,
        avg($"A0011B_DURING") as "A0011B_DURING",
        sum("A0011B_DAY_CNT") as "A0011B_DAY_CNT" ,
        avg($"A0011E_DURING") as "A0011E_DURING",
        sum("A0011E_DAY_CNT") as "A0011E_DAY_CNT" ,
        avg($"A0011J_DURING") as "A0011J_DURING",
        sum("A0011J_DAY_CNT") as "A0011J_DAY_CNT",
        sum("FIRST_LEVEL_CAP_CNT") as "FIRST_LEVEL_CAP_CNT",
        sum("SECOND_LEVEL_CAP_CNT") as "SECOND_LEVEL_CAP_CNT",
        sum("THIRD_LEVEL_CAP_CNT") as "THIRD_LEVEL_CAP_CNT",
        sum("FOURTH_LEVEL_CAP_CNT") as "FOURTH_LEVEL_CAP_CNT",
        sum("FIFTH_LEVEL_CAP_CNT") as "FIFTH_LEVEL_CAP_CNT",
        sum("FIRST_CONS_SORT_CNT") as "FIRST_CONS_SORT_CNT",
        sum("SECOND_CONS_SORT_CNT") as "SECOND_CONS_SORT_CNT",
        sum("THIRD_CONS_SORT_CNT") as "THIRD_CONS_SORT_CNT",
        sum("FOURTH_CONS_SORT_CNT") as "FOURTH_CONS_SORT_CNT",
        sum("FIFTH_CONS_SORT_CNT") as "FIFTH_CONS_SORT_CNT",
        sum("SIXTH_CONS_SORT_CNT") as "SIXTH_CONS_SORT_CNT",
        sum("SEVENTH_CONS_SORT_CNT") as "SEVENTH_CONS_SORT_CNT",
        sum("EIGHTH_CONS_SORT_CNT") as "EIGHTH_CONS_SORT_CNT",
        sum("NINTH_CONS_SORT_CNT") as "NINTH_CONS_SORT_CNT",
        sum("TENTH_CONS_SORT_CNT") as "TENTH_CONS_SORT_CNT",
        sum("ANTISTEALINGPROFIRST_CNT") as "ANTISTEALINGPROFIRST_CNT",
        sum("ANTISTEALINGPROSECOND_CNT") as "ANTISTEALINGPROSECOND_CNT",
        sum("ANTISTEALINGPROTHIRD_CNT") as "ANTISTEALINGPROTHIRD_CNT",
        sum("ANTISTEALINGPROFOURTH_CNT") as "ANTISTEALINGPROFOURTH_CNT"
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(">>>>>>>>>>>>>>>>>>>>>>>县统计数据量="+townData.count())
    val nullData1 = townData.filter($"ORG_TYPE".isNull)
    //1.1 获取县直属的数据
    val townDirectData = frame.filter($"ORG_TYPE" === "4").drop("ORG_TYPE").withColumn("ORG_TYPE",lit("05"))
      .select(
        "ORG_NO",
        "ORG_TYPE",
        "STAT_TIME",
        "RUN_CAP",
        "A00112_DURING",
        "A00112_DAY_CNT",
        "A00100_DURING",
        "A00100_DAY_CNT",
        "A0010J_DURING",
        "A0010J_DAY_CNT",
        "A00102_DURING",
        "A00102_DAY_CNT",
        "A00113_DURING",
        "A00113_DAY_CNT",
        "A00114_DURING",
        "A00114_DAY_CNT",
        "A00115_DURING",
        "A00115_DAY_CNT",
        "A00116_DURING",
        "A00116_DAY_CNT",
        "A00118_DURING",
        "A00118_DAY_CNT",
        "A00119_DURING",
        "A00119_DAY_CNT",
        "A0011B_DURING",
        "A0011B_DAY_CNT",
        "A0011E_DURING",
        "A0011E_DAY_CNT",
        "A0011J_DURING",
        "A0011J_DAY_CNT",
        "FIRST_LEVEL_CAP_CNT",
        "SECOND_LEVEL_CAP_CNT",
        "THIRD_LEVEL_CAP_CNT",
        "FOURTH_LEVEL_CAP_CNT",
        "FIFTH_LEVEL_CAP_CNT",
        "FIRST_CONS_SORT_CNT",
        "SECOND_CONS_SORT_CNT",
        "THIRD_CONS_SORT_CNT",
        "FOURTH_CONS_SORT_CNT",
        "FIFTH_CONS_SORT_CNT",
        "SIXTH_CONS_SORT_CNT",
        "SEVENTH_CONS_SORT_CNT",
        "EIGHTH_CONS_SORT_CNT",
        "NINTH_CONS_SORT_CNT",
        "TENTH_CONS_SORT_CNT",
        "ANTISTEALINGPROFIRST_CNT",
        "ANTISTEALINGPROSECOND_CNT",
        "ANTISTEALINGPROTHIRD_CNT",
        "ANTISTEALINGPROFOURTH_CNT"
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //2.获取市的数据
    val cityData = frame.as("frame").join(O_ORG_CHILDS_NEW.filter($"ORG_TYPE" === "03").as("org"),
      $"frame.ORG_NO" === $"org.CHILD_ORG_NO").groupBy($"org.ORG_NO",$"org.ORG_TYPE",$"frame.STAT_TIME")
      .agg(
        sum($"RUN_CAP") as "RUN_CAP",
        avg($"A00112_DURING") as "A00112_DURING",
        sum("A00112_DAY_CNT") as "A00112_DAY_CNT" ,
        avg($"A00100_DURING") as "A00100_DURING",
        sum("A00100_DAY_CNT") as "A00100_DAY_CNT" ,
        avg($"A0010J_DURING") as "A0010J_DURING",
        sum("A0010J_DAY_CNT") as "A0010J_DAY_CNT" ,
        avg($"A00102_DURING") as "A00102_DURING",
        sum("A00102_DAY_CNT") as "A00102_DAY_CNT" ,
        avg($"A00113_DURING") as "A00113_DURING",
        sum("A00113_DAY_CNT") as "A00113_DAY_CNT" ,
        avg($"A00114_DURING") as "A00114_DURING",
        sum("A00114_DAY_CNT") as "A00114_DAY_CNT" ,
        avg($"A00115_DURING") as "A00115_DURING",
        sum("A00115_DAY_CNT") as "A00115_DAY_CNT" ,
        avg($"A00116_DURING") as "A00116_DURING",
        sum("A00116_DAY_CNT") as "A00116_DAY_CNT" ,
        avg($"A00118_DURING") as "A00118_DURING",
        sum("A00118_DAY_CNT") as "A00118_DAY_CNT" ,
        avg($"A00119_DURING") as "A00119_DURING",
        sum("A00119_DAY_CNT") as "A00119_DAY_CNT" ,
        avg($"A0011B_DURING") as "A0011B_DURING",
        sum("A0011B_DAY_CNT") as "A0011B_DAY_CNT" ,
        avg($"A0011E_DURING") as "A0011E_DURING",
        sum("A0011E_DAY_CNT") as "A0011E_DAY_CNT" ,
        avg($"A0011J_DURING") as "A0011J_DURING",
        sum("A0011J_DAY_CNT") as "A0011J_DAY_CNT",
        sum("FIRST_LEVEL_CAP_CNT") as "FIRST_LEVEL_CAP_CNT",
        sum("SECOND_LEVEL_CAP_CNT") as "SECOND_LEVEL_CAP_CNT",
        sum("THIRD_LEVEL_CAP_CNT") as "THIRD_LEVEL_CAP_CNT",
        sum("FOURTH_LEVEL_CAP_CNT") as "FOURTH_LEVEL_CAP_CNT",
        sum("FIFTH_LEVEL_CAP_CNT") as "FIFTH_LEVEL_CAP_CNT",
        sum("FIRST_CONS_SORT_CNT") as "FIRST_CONS_SORT_CNT",
        sum("SECOND_CONS_SORT_CNT") as "SECOND_CONS_SORT_CNT",
        sum("THIRD_CONS_SORT_CNT") as "THIRD_CONS_SORT_CNT",
        sum("FOURTH_CONS_SORT_CNT") as "FOURTH_CONS_SORT_CNT",
        sum("FIFTH_CONS_SORT_CNT") as "FIFTH_CONS_SORT_CNT",
        sum("SIXTH_CONS_SORT_CNT") as "SIXTH_CONS_SORT_CNT",
        sum("SEVENTH_CONS_SORT_CNT") as "SEVENTH_CONS_SORT_CNT",
        sum("EIGHTH_CONS_SORT_CNT") as "EIGHTH_CONS_SORT_CNT",
        sum("NINTH_CONS_SORT_CNT") as "NINTH_CONS_SORT_CNT",
        sum("TENTH_CONS_SORT_CNT") as "TENTH_CONS_SORT_CNT",
        sum("ANTISTEALINGPROFIRST_CNT") as "ANTISTEALINGPROFIRST_CNT",
        sum("ANTISTEALINGPROSECOND_CNT") as "ANTISTEALINGPROSECOND_CNT",
        sum("ANTISTEALINGPROTHIRD_CNT") as "ANTISTEALINGPROTHIRD_CNT",
        sum("ANTISTEALINGPROFOURTH_CNT") as "ANTISTEALINGPROFOURTH_CNT"
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(">>>>>>>>>>>>>>>>>>>>>>>市统计数据量="+cityData.count())
    val nullData2 = cityData.filter($"ORG_TYPE".isNull)
    println(">>>>>>>>>>>>>>>>>>>>>>县为null的数据量="+nullData2.count())
    //2.1 获取市直属数据
    val cityDirectData = frame.filter($"ORG_TYPE" === "3").drop("ORG_TYPE").withColumn("ORG_TYPE",lit("04"))
      .select(
        "ORG_NO",
        "ORG_TYPE",
        "STAT_TIME",
        "RUN_CAP",
        "A00112_DURING",
        "A00112_DAY_CNT",
        "A00100_DURING",
        "A00100_DAY_CNT",
        "A0010J_DURING",
        "A0010J_DAY_CNT",
        "A00102_DURING",
        "A00102_DAY_CNT",
        "A00113_DURING",
        "A00113_DAY_CNT",
        "A00114_DURING",
        "A00114_DAY_CNT",
        "A00115_DURING",
        "A00115_DAY_CNT",
        "A00116_DURING",
        "A00116_DAY_CNT",
        "A00118_DURING",
        "A00118_DAY_CNT",
        "A00119_DURING",
        "A00119_DAY_CNT",
        "A0011B_DURING",
        "A0011B_DAY_CNT",
        "A0011E_DURING",
        "A0011E_DAY_CNT",
        "A0011J_DURING",
        "A0011J_DAY_CNT",
        "FIRST_LEVEL_CAP_CNT",
        "SECOND_LEVEL_CAP_CNT",
        "THIRD_LEVEL_CAP_CNT",
        "FOURTH_LEVEL_CAP_CNT",
        "FIFTH_LEVEL_CAP_CNT",
        "FIRST_CONS_SORT_CNT",
        "SECOND_CONS_SORT_CNT",
        "THIRD_CONS_SORT_CNT",
        "FOURTH_CONS_SORT_CNT",
        "FIFTH_CONS_SORT_CNT",
        "SIXTH_CONS_SORT_CNT",
        "SEVENTH_CONS_SORT_CNT",
        "EIGHTH_CONS_SORT_CNT",
        "NINTH_CONS_SORT_CNT",
        "TENTH_CONS_SORT_CNT",
        "ANTISTEALINGPROFIRST_CNT",
        "ANTISTEALINGPROSECOND_CNT",
        "ANTISTEALINGPROTHIRD_CNT",
        "ANTISTEALINGPROFOURTH_CNT"
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //3.获取省的数据
    val provinceData = frame.as("frame").join(O_ORG_CHILDS_NEW.filter($"ORG_TYPE" === "02").as("org"),
      $"frame.ORG_NO" === $"org.CHILD_ORG_NO").groupBy($"org.ORG_NO",$"org.ORG_TYPE",$"frame.STAT_TIME")
      .agg(
        sum($"RUN_CAP") as "RUN_CAP",
        avg($"A00112_DURING") as "A00112_DURING",
        sum("A00112_DAY_CNT") as "A00112_DAY_CNT" ,
        avg($"A00100_DURING") as "A00100_DURING",
        sum("A00100_DAY_CNT") as "A00100_DAY_CNT" ,
        avg($"A0010J_DURING") as "A0010J_DURING",
        sum("A0010J_DAY_CNT") as "A0010J_DAY_CNT" ,
        avg($"A00102_DURING") as "A00102_DURING",
        sum("A00102_DAY_CNT") as "A00102_DAY_CNT" ,
        avg($"A00113_DURING") as "A00113_DURING",
        sum("A00113_DAY_CNT") as "A00113_DAY_CNT" ,
        avg($"A00114_DURING") as "A00114_DURING",
        sum("A00114_DAY_CNT") as "A00114_DAY_CNT" ,
        avg($"A00115_DURING") as "A00115_DURING",
        sum("A00115_DAY_CNT") as "A00115_DAY_CNT" ,
        avg($"A00116_DURING") as "A00116_DURING",
        sum("A00116_DAY_CNT") as "A00116_DAY_CNT" ,
        avg($"A00118_DURING") as "A00118_DURING",
        sum("A00118_DAY_CNT") as "A00118_DAY_CNT" ,
        avg($"A00119_DURING") as "A00119_DURING",
        sum("A00119_DAY_CNT") as "A00119_DAY_CNT" ,
        avg($"A0011B_DURING") as "A0011B_DURING",
        sum("A0011B_DAY_CNT") as "A0011B_DAY_CNT" ,
        avg($"A0011E_DURING") as "A0011E_DURING",
        sum("A0011E_DAY_CNT") as "A0011E_DAY_CNT" ,
        avg($"A0011J_DURING") as "A0011J_DURING",
        sum("A0011J_DAY_CNT") as "A0011J_DAY_CNT",
        sum("FIRST_LEVEL_CAP_CNT") as "FIRST_LEVEL_CAP_CNT",
        sum("SECOND_LEVEL_CAP_CNT") as "SECOND_LEVEL_CAP_CNT",
        sum("THIRD_LEVEL_CAP_CNT") as "THIRD_LEVEL_CAP_CNT",
        sum("FOURTH_LEVEL_CAP_CNT") as "FOURTH_LEVEL_CAP_CNT",
        sum("FIFTH_LEVEL_CAP_CNT") as "FIFTH_LEVEL_CAP_CNT",
        sum("FIRST_CONS_SORT_CNT") as "FIRST_CONS_SORT_CNT",
        sum("SECOND_CONS_SORT_CNT") as "SECOND_CONS_SORT_CNT",
        sum("THIRD_CONS_SORT_CNT") as "THIRD_CONS_SORT_CNT",
        sum("FOURTH_CONS_SORT_CNT") as "FOURTH_CONS_SORT_CNT",
        sum("FIFTH_CONS_SORT_CNT") as "FIFTH_CONS_SORT_CNT",
        sum("SIXTH_CONS_SORT_CNT") as "SIXTH_CONS_SORT_CNT",
        sum("SEVENTH_CONS_SORT_CNT") as "SEVENTH_CONS_SORT_CNT",
        sum("EIGHTH_CONS_SORT_CNT") as "EIGHTH_CONS_SORT_CNT",
        sum("NINTH_CONS_SORT_CNT") as "NINTH_CONS_SORT_CNT",
        sum("TENTH_CONS_SORT_CNT") as "TENTH_CONS_SORT_CNT",
        sum("ANTISTEALINGPROFIRST_CNT") as "ANTISTEALINGPROFIRST_CNT",
        sum("ANTISTEALINGPROSECOND_CNT") as "ANTISTEALINGPROSECOND_CNT",
        sum("ANTISTEALINGPROTHIRD_CNT") as "ANTISTEALINGPROTHIRD_CNT",
        sum("ANTISTEALINGPROFOURTH_CNT") as "ANTISTEALINGPROFOURTH_CNT"
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val result = supplyData.union(townData).union(townDirectData).union(cityData).union(cityDirectData).union(provinceData).toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>结果=")
    result.show(2)
    supplyData.unpersist()
    townData.unpersist()
    townDirectData.unpersist()
    cityData.unpersist()
    cityDirectData.unpersist()
    provinceData.unpersist()
   /* if("day".equals(dateType))
    kudu.insert("A_ORG_VIOLATE_D",result,dataDate)
    else*/
    kudu.insert13("A_ORG_VIOLATE_M",result,dataDate)
    result.unpersist()
    true
  }
}
