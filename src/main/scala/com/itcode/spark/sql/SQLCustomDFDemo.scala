package com.itcode.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/9/1 15:56.
  * Email:466210864@qq.com
  */
object SQLCustomDFDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", "hadoop")

    import org.apache.spark.sql.types._
    // 用字符串编码模式
    val schemaString = "custom_name custom_id costom_money"
    // 用模式字符串生成模式对象
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // 将RDD（sc.textFile("hdfs://mini1:9000/person.txt")）记录转化成Row。
    val rowRDD = sc.textFile("hdfs://mini1:9000/person.txt").map(_.split(",")).map(p => Row(p(0).trim, p(1), p(2)))

    // 将模式应用于RDD对象。
    val dfCustomers = sqlContext.createDataFrame(rowRDD, schema)
    //    dfCustomers.registerTempTable("person")//deprecated
    dfCustomers.createOrReplaceTempView("person")

    dfCustomers.show() //显示DataFrame的内容
    dfCustomers.printSchema() //打印DataFrame模式
    dfCustomers.select("custom_name").show() //选择名称列
    dfCustomers.select("custom_name", "custom_id").show()
    dfCustomers.filter(dfCustomers("custom_id").equalTo(26)).show() //根据条件过滤
    dfCustomers.groupBy("custom_id").count().show() //根据id统计数量

    val df = sqlContext.sql("select * from person where costom_money >= 24 order by costom_money desc limit 2")
    df.show()
    df.write.json("hdfs://mini1:9000/personSTout")
    sc.stop()
  }
}

case class PersonCustom(name: String, id: Long, money: Int)