package com.itcode.spark.streaming

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/21 14:19.
  * Email:466210864@qq.com
  * 读取mini1使用命令 nc -lk 9999发送的数据
  * 统计单词数量，但不会将结果相加
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    LoggerLevelsUtils.setStreamingLogLevels()
    //创建SparkConf并设置为本地模式运行，注意local[2]代表开两个线程
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    //设置DStream批次时间间隔为2秒
    val ssc = new StreamingContext(sc, Seconds(5))
    //通过网络读取数据
    val lines = ssc.socketTextStream("mini1", 9999)
    //DStream 是一个特殊的RDD
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //打印结果
    result.print()
    //开始计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }

}
