package main.scala.sparkTask1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object Submit2 {
  private val format = new SimpleDateFormat("yyyyMMdd")
  private val format1 = new SimpleDateFormat("yyyyMM")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("conf")
    val context = new SparkContext(conf)
    val hc = new HiveContext(context)
    val date = new Date()
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val cal2 = Calendar.getInstance()
    cal2.setTime(date)
    cal2.add(Calendar.MONTH,-1)
    if (args.length == 0) {
      println(">>>>>>>>>>>>>>>>>>>>走的是无时间类型无具体时间的方法")
      AOrgViolateD.AOrgViolateD(hc,  format.format(cal.getTime))
      AOrgViolateM.AOrgViolateM(hc,  format1.format(cal2.getTime))
    } else
      args(0) match {
        case "day" => {
          if (args.length == 1) {
            println(">>>>>>>>>>>>>>>>>>>>走的是按日无具体时间的方法")
            AOrgViolateD.AOrgViolateD(hc,  format.format(cal.getTime))
          } else
            println(">>>>>>>>>>>>>>>>>>>>走的是按日有具体时间的方法")
            AOrgViolateD.AOrgViolateD(hc,  args(1))
        }
        case "month" => {
          if (args.length == 1) {
            println(">>>>>>>>>>>>>>>>>>>>走的是按月无具体时间的方法")
            AOrgViolateM.AOrgViolateM(hc,  format1.format(cal2.getTime))
          }
          else
            println(">>>>>>>>>>>>>>>>>>>>走的是按月无具体时间的方法")
            AOrgViolateM.AOrgViolateM(hc,  args(1))
        }
      }
  }


}
