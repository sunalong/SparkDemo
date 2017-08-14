package com.itcode.spark

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/14 18:19.
  * Email:466210864@qq.com
  * 取出学科点击前三
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（URL,1)
    val rdd1 = sc.textFile("/Users/along/ATest/itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    println("rdd1:" + rdd1.collect().toBuffer)

    val rdd2 = rdd1.reduceByKey(_ + _)
    println("rdd2:" + rdd2.collect().toBuffer)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })
    println("rdd3:" + rdd3.collect().toBuffer)

    val rdd4 = rdd3.groupBy(_._1).mapValues(it=>{
      it.toList.sortBy(_._3).reverse.take(3)//在java中排序，数据量大时会爆
    })
    println("rdd4:" + rdd4.collect().toBuffer)

    sc.stop()
  }

}
