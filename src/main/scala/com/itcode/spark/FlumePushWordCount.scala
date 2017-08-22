package com.itcode.spark

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by along on 17/8/21 20:36.
  * Email:466210864@qq.com
  */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    val host = "sunalongMBP"
    val port = 8888
    LoggerLevelsUtils.setStreamingLogLevels()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //推送方式：flume向spark发送数据
    val flumeStream = FlumeUtils.createStream(ssc, host, port)
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_, 1))
    val result = words.reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
