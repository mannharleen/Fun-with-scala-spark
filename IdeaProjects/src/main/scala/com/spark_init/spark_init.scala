package com.spark_init
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.hive.HiveContext

object spark_init{
  def init(): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
  }
}
