package com.itcode.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by along on 17/8/14 18:19.
  * Email:466210864@qq.com
  * 取出学科点击前三
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    //从数据库中加载规则
    val arr = Array("java.itcast.cn", "php.itcast.cn", "net.itcast.cn")
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

    partitionTest(rdd2)

//    sortInScala(rdd3)
//    sortInRDD(arr, rdd3)


    sc.stop()
  }


  private def partitionTest(rdd2: RDD[(String, Int)]) = {
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })
    println("rdd3:" + rdd3.collect().toBuffer)


    val instituteNameArr = rdd3.map(_._1).distinct().collect()
    val hostPartioner = new HostPartioner(instituteNameArr)
    val rdd4 = rdd3.partitionBy(hostPartioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    rdd4.saveAsTextFile("/Users/along/ATest/out1")
  }

  /**
    * 在RDD中排序，如果数据量大，会放在文件中，不会让内存爆
    *
    * @param arr
    * @param rdd3
    */
  private def sortInRDD(arr: Array[String], rdd3: RDD[(String, String, Int)]) = {
    for (ins <- arr) {
      val rdd = rdd3.filter(_._1 == ins)
      val result = rdd.sortBy(_._3, false).take(3)
      println("result:" + result.toBuffer)
    }
  }

  private def sortInScala(rdd3: RDD[(String, String, Int)]) = {
    val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3) //在java中排序，数据量大时会爆
    })
    println("rdd4:" + rdd4.collect().toBuffer)
  }
}

/**
  * 决定数据到哪个分区里面
  *
  * @param instituteArr
  */
class HostPartioner(instituteArr: Array[String]) extends Partitioner {
  val partitionerMap = new mutable.HashMap[String, Int]()
  var count = 0
  for (i <- instituteArr) {
    partitionerMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = instituteArr.length

  override def getPartition(key: Any): Int = {
    partitionerMap.getOrElse(key.toString, 0)
  }
}
