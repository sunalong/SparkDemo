package com.itcode.spark

/**
  * Created by along on 17/8/16 10:42.
  * Email:466210864@qq.com
  */
object IPDemo {

  def ip2Long(ip: String) = {
//    val fragments = ip.split("[.]")
    val fragments = ip.split("\\.")//同上
    var ipNum = 0L
    for(i<-0 until fragments.length){
      ipNum = fragments(i).toLong|ipNum<<8L
    }
    ipNum
  }

  def main(args: Array[String]): Unit = {
    val ip = "180.168.126.245"
    val ipNum = ip2Long(ip)
    println("ip:"+ip+"-->ipNum:"+ipNum)
  }

}
