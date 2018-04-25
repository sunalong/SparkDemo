package com.itcode.spark.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by along on 17/8/26 14:28.
  * Email:466210864@qq.com
  */
object TimeUtils {
  val simeDataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calender = Calendar.getInstance()

  //  def apply: TimeUtils = new TimeUtils()
  def apply(time: String) = {
    calender.setTime(simeDataFormat.parse(time))
    calender.getTimeInMillis
  }

  def getCertainDayTime(amount: Int): Long = {
    calender.add(Calendar.DATE, amount)
    val time = calender.getTimeInMillis
    calender.add(Calendar.DATE, -amount)
    time
  }
}
