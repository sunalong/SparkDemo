package com.itcode.spark

import java.net.InetSocketAddress

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by along on 17/8/22 12:39.
  * Email:466210864@qq.com
  */
object FlumePullWordCount {
  def main(args: Array[String]): Unit = {
    LoggerLevelsUtils.setStreamingLogLevels()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从flume中拉取数据
    val address = Seq(new InetSocketAddress("mini1", 8888))
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val words = flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ")).map((_, 1))
    val result = words.reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
