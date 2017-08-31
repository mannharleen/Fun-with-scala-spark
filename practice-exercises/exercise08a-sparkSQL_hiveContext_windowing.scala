/*
Problem Statement:
Get top N stocks by volume for each year

  Function should sort data in descending order and return top 5 stocks
  Use both rank and dense_rank
  Use HiveContext

*/

package com.exercise08a

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.hive.HiveContext

object sparkSQL_hiveContext_windowing {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("xx").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    //create and load temp table
    hiveContext.sql("use nyse_db")
    hiveContext.sql("create temporary table nyse_temp (name String, date String, open Double, high Double, low Double, close Double, volume BigInt) ROW FORMAT delimited fields terminated by ',' ")
    hiveContext.sql("load data local inpath 'D:\\OneDrive - Parkway Pantai\\BIG DATA\\data\\nyse\\nyse_data' into table nyse_temp")

    //create managed table with partition
    hiveContext.sql("create table nyse (name String, date String, open Double, high Double, low Double, close Double, volume BigInt) partitioned by (year String)")
    // -- note: since date and month have diff column names its fine. if we were to use date as a whole as partitioned by column, make sure to rename original date column to something else.
    // -- this is necessary since partitioned by create a pseudo column with that name

    //use insert statement to perform dynamic partitioning from temp table
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveContext.sql("insert into table nyse partition(year) select name,date,open,high,low,close,volume,substr(date,8,4) as year from nyse_temp")
    // -- most important to note: 'month' is used for dynamic partition and must be the last column in the select statement

    //rank:
    hiveContext.sql("select month, name, rank_ from (select month, name, rank() over (partition by month order by volume desc) as rank_ from nyse) q1 where rank_<=5").collect.foreach(println)
    //dense rank:
    hiveContext.sql("select month, name, rank_ from (select month, name, dense_rank() over (partition by month order by volume desc) as rank_ from nyse) q1 where rank_<=5").collect.foreach(println)


  }

}
