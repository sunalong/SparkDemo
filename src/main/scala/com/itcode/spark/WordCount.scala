package com.itcode.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/13 16:10.
  * Email:466210864@qq.com
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myWC").setMaster("local")
    //通向spark集群的入口
    val sc = new SparkContext(conf)
//    wordCount(args, sc)
    combineByKeyTest(sc)//
    sc.stop()
  }

  private def wordCount(args: Array[String], sc: SparkContext) = {
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _).sortBy(_._2, false).saveAsTextFile(args(1))
  }

  private def combineByKeyTest(sc: SparkContext) = {
    val rdd4 = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val rdd5 = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val rdd6 = rdd5.zip(rdd4)
    println("rdd6:" + rdd6.collect().toBuffer)
    val rdd7 = rdd6.combineByKey(List(_), (x: List[String], y: String) => x :+ y, (m: List[String], n: List[String]) => m ++ n)
    println("rdd7:" + rdd7.collect().toBuffer)
  }
}

