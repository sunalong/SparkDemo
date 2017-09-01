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
    personDf.show()//显示DataFrame的内容
    personDf.printSchema()//打印DataFrame模式
    personDf.select("name").show()//选择名称列
    personDf.select("name","id").show()
    personDf.filter(personDf("id").equalTo(26)).show()//根据条件过滤
    personDf.groupBy("id").count().show()//根据id统计数量
    sqlContext.sql("select * from person where money >= 200 order by money desc limit 20").show()
//    sc.stop()
  }
}

case class Person(name: String, id: Long, money: Int)