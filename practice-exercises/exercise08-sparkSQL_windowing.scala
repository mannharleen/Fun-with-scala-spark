package com.exercise08

/*
Problem Statement: (wrt nyse dataaset)
Get top 10 stocks by volume for each day
  - sparse rank
  - dense rank

  Use sparkSQL API
  Use sparkSQL in native context (must be spark 2.0+)

 */

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext,functions}

case class nyse_data_case_class(
                               name:String,
                               date:String,
                               open:Double,
                               high:Double,
                               low:Double,
                               close:Double,
                               volume:Long
                               )

object sparkSQL_windowing {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sparkSQL_windowing").setMaster("local[2]"))
    val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("spark.sql.shuffle.partitions","6")
    //---job running v slow - tweak partitions and Master
    import sqlContext.implicits._     //required for toDF
    //sc.setLogLevel("ERROR")

    val param1 = "26-Nov-2010"
    val rdd_nyse = sc.textFile("s3n://XXX:YYY@modharleen/bigdata/data/nyse_data/",6).
      filter(x=> x.contains(param1))

    val df_nyse = rdd_nyse.map(x=> {
      val t = x.split(",")
      nyse_data_case_class(t(0),t(1),t(2).toDouble,t(3).toDouble,t(4).toDouble,t(5).toDouble,t(6).toLong)
    }).toDF()

    df_nyse.registerTempTable("nyse")
    //important: you must be using Spark 2.0+ for using analytical functions in spark native context
    val sql1 = sqlContext.sql("select date, name, rank_ from (select date, name, rank over (partition by date order by volume desc) rank_ from nyse) q1 where rank_ <=10")
    val sql2 = sqlContext.sql("select date, name, rank_ from (select date, name, dense_rank over (partition by date order by volume desc) rank_ from nyse) q1 where rank_ <=10")

    sql1.collect()
    sql2.collect()

  }
}
