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
    System.setProperty("user.name", "hadoop")
    val personRdd = sc.textFile("hdfs://mini1:9000/person.txt").map(line => {
      val fields = line.split(",")
      Person(fields(0), fields(1).toLong, fields(2).toInt)
    })
    import sqlContext.implicits._
    //导入隐式转换，如果不导入，无法将RDD转换成DataFrame
    val personDf = personRdd.toDF
    personDf.registerTempTable("person")
    personDf.show()//显示DataFrame的内容
    personDf.printSchema()//打印DataFrame模式
    personDf.select("name").show()//选择名称列
    personDf.select("name","id").show()
    personDf.filter(personDf("id").equalTo(26)).show()//根据条件过滤
    personDf.groupBy("id").count().show()//根据id统计数量
    val df = sqlContext.sql("select * from person where age >= 24 order by age desc limit 2")
    df.show()
    df.write.json("hdfs://mini1:9000/personOut")//将结果以json的方式存储到指定位置
    sc.stop()
  }
}

case class Person(name: String, id: Long, age: Int)