package com.itcode.spark.streaming

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Created by along on 17/8/22 17:54.
  * Email:466210864@qq.com
  */
object KafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap {
      case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i))
    }
  }

  def main(args: Array[String]): Unit = {
    LoggerLevelsUtils.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("ATest/fuck")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1))
      .updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
