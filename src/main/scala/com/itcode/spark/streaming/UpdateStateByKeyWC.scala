package com.itcode.spark.streaming

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by along on 17/8/21 18:15.
  * Email:466210864@qq.com
  * 读取mini1使用命令 nc -lk 9999发送的数据
  * 统计单词数量，同时将结果相加
  */
object UpdateStateByKeyWC {
  /**
    * 函数原型：(Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
    * K：单词
    * Seq[V]：单词在当前批次出现的次数
    * Option[S]：历史结果
    *
    * @return
    */
  def updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map {
      case (word, currentCount, historyCount) =>
        (word, currentCount.sum + historyCount.getOrElse(0))
    }
  }

  def main(args: Array[String]): Unit = {
    LoggerLevelsUtils.setStreamingLogLevels()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    //做checkpoint写入共享存储中
    sc.setCheckpointDir("fuckMeCKDir")
    val ssc = new StreamingContext(sc, Seconds(5))
    val ds = ssc.socketTextStream("mini1", 9999)
    //updateStateByKey结果可以累加，但是需要传入一个自定义的累加函数updateFunc
    val result = ds.flatMap(_.split(" ")).map((_, 1))
      .updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
