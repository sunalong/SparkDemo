package com.itcode.spark

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by along on 17/8/16 10:42.
  * Email:466210864@qq.com
  * 根据给定的ip，查找归属地
  */
object IPDemo {

  def ip2Long(ip: String) = {
    //    val fragments = ip.split("[.]")
    val fragments = ip.split("\\.")
    //同上
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readData(path: String) = {
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var strLine: String = null
    var flag = true
    val linesArr = new ArrayBuffer[String]()
    while (flag) {
      strLine = br.readLine()
      if (strLine != null)
        linesArr += strLine
      else
        flag = false
    }
    linesArr
  }

  def binarySearch(linesArr: ArrayBuffer[String], ipNum: Long): Int = {
    var low = 0
    var high = linesArr.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      val lineSplitArr = linesArr(middle).split("\\|")
      if ((ipNum >= lineSplitArr(2).toLong) && (ipNum <= lineSplitArr(3).toLong))
        return middle
      if (ipNum < lineSplitArr(2).toLong)
        high = middle - 1
      else
        low = middle + 1
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val ip = "180.168.126.245"
    val ipNum = ip2Long(ip)
    println("ip:" + ip + "-->ipNum:" + ipNum)
    val linesArr = readData("/Users/along/ATest/ip.txt")
    val index = binarySearch(linesArr, ipNum)
    println(linesArr(index))
  }

}
