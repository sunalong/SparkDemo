package com.itcode.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadJsonFromHDFS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", "hadoop")


    //直接读取json格式文件
    val df1 = sqlContext.read.json("hdfs://mini1:9000/events-.1523268024993")
    //通过load读取json格式文件，需要指定格式，不指定默认读取的是parquet格式文件
    //sqlContext.read.format("json").load("hdfs://mini1:9000/events-.1523268024993")
    df1.printSchema()//打印DataFrame模式
    df1.show()//显示DataFrame的内容
    //    root
    //    |-- @timestamp: string (nullable = true)
    //    |-- beat: struct (nullable = true)
    //    |    |-- hostname: string (nullable = true)
    //    |    |-- name: string (nullable = true)
    //    |    |-- version: string (nullable = true)
    //    |-- fields: struct (nullable = true)
    //    |    |-- idc: string (nullable = true)
    //    |    |-- ip: string (nullable = true)
    //    |    |-- log_type: string (nullable = true)
    //    |-- input_type: string (nullable = true)
    //    |-- message: string (nullable = true)
    //    |-- offset: long (nullable = true)
    //    |-- source: string (nullable = true)
    //    |-- type: string (nullable = true)

    df1.registerTempTable("ptLoginLog_temptab")
    val teenagers = sqlContext.sql("select * from ptLoginLog_temptab")
//    val teenagers = sqlContext.sql("select name from ptLoginLog_temptab where age > 13 and age <19")
    teenagers.write.parquet("hdfs://mini1:9000/jsonout")

    sc.stop()
  }
}
