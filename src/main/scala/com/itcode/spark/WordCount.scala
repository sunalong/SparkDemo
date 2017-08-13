package com.itcode.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/13 16:10.
  * Email:466210864@qq.com
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myWC")
    //通向spark集群的入口
    val sc= new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1))
      .reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))
    sc.stop()
  }

}

