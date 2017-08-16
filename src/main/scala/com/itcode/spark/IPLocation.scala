package com.itcode.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/16 11:24.
  * Email:466210864@qq.com
  */
object IPLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("IPLocation")
    val sc = new SparkContext(conf)

    val ipRulesRdd = sc.textFile("/Users/along/ATest/ip.txt").map(line => {
      val fields = line.split("\\|")
      val startNum = fields(2)
      val endNum = fields(3)
      val province = fields(6)
      (startNum, endNum, province)
    })
    //全部的IP映射规则
    val ipRulesArray = ipRulesRdd.collect()
    //广播规则
    val ipRulesBroadcast = sc.broadcast(ipRulesArray)
    //加载要处理的数据
    val ipsRDD = sc.textFile("/Users/along/ATest/access_log").map(line => {
      val fields = line.split("\\|")
      fields(1)
    })
    val result = ipsRDD.map(ip => {
      val ipNum = IPDemo.ip2Long(ip)
      val index = IPDemo.binarySearch(ipRulesBroadcast.value, ipNum)
      val info = ipRulesBroadcast.value(index)
      info
    })

    println("result:" + result.collect().toBuffer)
    sc.stop()
  }
}
