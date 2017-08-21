package com.itcode.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
  * Created by along on 17/8/21 14:59.
  * Email:466210864@qq.com
  */
object LoggerLevelsUtils extends Logging{
  def setStreamingLogLevels(): Unit ={
    val log4jInitialized = Logger.getRootLogger().getAllAppenders.hasMoreElements
    if(!log4jInitialized){
      logInfo("设置等级为warn")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }


}
