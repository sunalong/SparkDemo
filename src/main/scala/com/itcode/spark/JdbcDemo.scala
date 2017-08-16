package com.itcode.spark

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/16 12:13.
  * Email:466210864@qq.com
  */
object JdbcDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://mini1:3306/bigdata", "root", "hadoop")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "select * from ip where id>=? and id<=?",
      1, 4, 2,
      r => {
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )
    val jrdd = jdbcRDD.collect()
    println("jrdd:" + jrdd.toBuffer)
    sc.stop()
  }

}
