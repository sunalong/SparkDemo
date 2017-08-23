package com.itcode.spark

import com.itcode.spark.utils.LoggerLevelsUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * Created by along on 17/8/23 17:49.
  * Email:466210864@qq.com
  */
object WindowOps {
  def main(args: Array[String]): Unit = {
    LoggerLevelsUtils.setStreamingLogLevels()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Milliseconds(5000))
    val lines = ssc.socketTextStream("mini1",9999)
    val pairs = lines.flatMap(_.split(" ")).map((_,1))
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(15),Seconds(10))

    val a = windowedWordCounts.map(_._2).reduce(_+_)
    a.foreachRDD(rdd=>{
      println("rdd.take(0):"+rdd.take(0))
    })
    a.print()

    windowedWordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
