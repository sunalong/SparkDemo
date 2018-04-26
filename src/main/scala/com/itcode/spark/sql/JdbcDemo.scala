package com.itcode.spark.sql

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by along on 17/8/16 12:13.
  * Email:466210864@qq.com
  */
object JdbcDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    getDataFromMysql(sc)
//    writeToMysql(sc)

    //停止SparkContext
    sc.stop()
  }

  /**
    * 写数据到mysql
    * @param sc
    */
  private def writeToMysql(sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    //通过并行化创建RDD
    val personRDD = sc.parallelize(Array("tom 1 15", "jerry 4 24", "kitty 6 27")).map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("id", IntegerType, true),
        StructField("age", IntegerType, true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0), p(1).toInt, p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "hadoop")
    //将数据追回到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://mini1:3306/bigdata", "bigdata.person", prop)
  }

  /**
    * 从mysql中获取数据
    * @param sc
    */
  private def getDataFromMysql(sc: SparkContext) = {
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://mini1:3306/bigdata", "root", "hadoop")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "select * from person where id>=? and id<=?",
      1, 4, 2,
      r => {
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )
    val jrdd = jdbcRDD.collect()
    println("jrdd:" + jrdd.toBuffer)
  }
}
