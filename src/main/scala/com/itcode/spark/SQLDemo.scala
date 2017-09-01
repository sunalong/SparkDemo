package com.itcode.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by along on 17/9/1 11:52.
  * Email:466210864@qq.com
  */
object SQLDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", "bigdata")

    val personRdd = sc.textFile("hdfs://mini1:9000/person.txt").map(line => {
      val fields = line.split(",")
      Person(fields(0), fields(1).toLong, fields(2).toInt)
    })

    import sqlContext.implicits._
    val personDf = personRdd.toDF
    personDf.registerTempTable("person")
    sqlContext.sql("select * from person where money >= 200 order by money desc limit 20").show()
    sc.stop()
  }
}

case class Person(name: String, id: Long, money: Int)