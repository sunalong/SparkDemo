package com.itcode.spark.game

import com.itcode.spark.utils.{FilterUtils, TimeUtils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/26 14:26.
  * Email:466210864@qq.com
  */
object GameKPI {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-02 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(+1)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //切分之后的数据
    val splitedLogs = sc.textFile("/Users/along/ATest/GameLogs/GameLog.txt").map(_.split("\\|"))
    //过滤后并缓冲
    val fileteredLogs = splitedLogs.filter(fields => FilterUtils.filterByTime(fields, beginTime, endTime)).cache()

    //日新增用户数，Daily New Users 缩写DNU
    val dnu = fileteredLogs.filter(fields => FilterUtils.filterByType(fields, EventType.REGISTER)).count()

    println("dnu:" + dnu)
    sc.stop()
  }

}
