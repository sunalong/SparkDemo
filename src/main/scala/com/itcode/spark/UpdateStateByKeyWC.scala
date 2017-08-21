package com.itcode.spark

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by along on 17/8/21 18:15.
  * Email:466210864@qq.com
  */
object UpdateStateByKeyWC {
  //                    (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  def updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map { case (word, currentCount, historyCount) => (word, currentCount.sum + historyCount.getOrElse(0)) }
  }

  def main(args: Array[String]): Unit = {
    LoggerLevelsUtils.setStreamingLogLevels()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("fuckMeCKDir")
    val ssc = new StreamingContext(sc, Seconds(5))
    val ds = ssc.socketTextStream("mini1", 8888)
    val result = ds.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
